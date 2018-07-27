/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.netflix.conductor.core.events;

import com.google.common.base.Predicate;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.events.EventExecution.Status;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.events.EventHandler.Action;
import com.netflix.conductor.common.utils.RetryUtil;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.service.ExecutionService;
import com.netflix.conductor.service.MetadataService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Viren
 * Event Processor is used to dispatch actions based on the incoming events to execution queue.
 */
@Singleton
public class EventProcessor {

    private static Logger logger = LoggerFactory.getLogger(EventProcessor.class);
    private static final String className = EventProcessor.class.getSimpleName();
    private static final int RETRY_COUNT = 3;

    private final MetadataService metadataService;
    private final ExecutionService executionService;
    private final ActionProcessor actionProcessor;

    private ForkJoinPool customForkJoinPool;
    private final Map<String, ObservableQueue> eventToQueueMap = new ConcurrentHashMap<>();

    @Inject
    public EventProcessor(ExecutionService executionService, MetadataService metadataService,
                          ActionProcessor actionProcessor, Configuration config) {
        this.executionService = executionService;
        this.metadataService = metadataService;
        this.actionProcessor = actionProcessor;

        int executorThreadCount = config.getIntProperty("workflow.event.processor.thread.count", 2);
        if (executorThreadCount > 0) {
            customForkJoinPool = new ForkJoinPool(executorThreadCount);
            refresh();
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(this::refresh, 60, 60, TimeUnit.SECONDS);
        } else {
            logger.warn("Event processing is DISABLED. executorThreadCount set to {}", executorThreadCount);
        }
    }

    /**
     * @return Returns a map of queues which are active.  Key is event name and value is queue URI
     */
    public Map<String, String> getQueues() {
        Map<String, String> queues = new HashMap<>();
        eventToQueueMap.forEach((key, value) -> queues.put(key, value.getName()));
        return queues;
    }

    public Map<String, Map<String, Long>> getQueueSizes() {
        Map<String, Map<String, Long>> queues = new HashMap<>();
        eventToQueueMap.forEach((key, value) -> {
            Map<String, Long> size = new HashMap<>();
            size.put(value.getName(), value.size());
            queues.put(key, size);
        });
        return queues;
    }

    private void refresh() {
        try {
            Set<String> events = metadataService.getEventHandlers().stream()
                    .map(EventHandler::getEvent)
                    .collect(Collectors.toSet());

            List<ObservableQueue> createdQueues = new LinkedList<>();
            events.forEach(event -> eventToQueueMap.computeIfAbsent(event, s -> {
                        ObservableQueue q = EventQueues.getQueue(event, false);
                        createdQueues.add(q);
                        return q;
                    }
            ));

            // start listening on all of the created queues
            createdQueues.stream()
                    .filter(Objects::nonNull)
                    .forEach(this::listen);

        } catch (Exception e) {
            Monitors.error(className, "refresh");
            logger.error("refresh event queues failed", e);
        }
    }

    private void listen(ObservableQueue queue) {
        queue.observe().subscribe((Message msg) -> handle(queue, msg));
    }

    private void handle(ObservableQueue queue, Message msg) {
        try {
            String payload = msg.getPayload();
            executionService.addMessage(queue.getName(), msg);

            String event = queue.getType() + ":" + queue.getName();
            logger.debug("Evaluating message: {} for event: {}", msg.getId(), event);

            List<EventHandler> eventHandlerList = metadataService.getEventHandlersForEvent(event, true);

            int i = 0;
            for (EventHandler eventHandler : eventHandlerList) {
                EventExecution eventExecution = new EventExecution(msg.getId() + "_" + i, msg.getId());
                eventExecution.setCreated(System.currentTimeMillis());
                eventExecution.setEvent(eventHandler.getEvent());
                eventExecution.setName(eventHandler.getName());

                String condition = eventHandler.getCondition();
                if (StringUtils.isNotEmpty(condition)) {
                    logger.debug("Checking condition: {} for event: {}", condition, event);
                    Boolean success = ScriptEvaluator.evalBool(condition, payload);
                    if (!success) {
                        eventExecution.setStatus(Status.SKIPPED);
                        eventExecution.getOutput().put("msg", payload);
                        eventExecution.getOutput().put("condition", condition);
                        executionService.addEventExecution(eventExecution);
                        continue;
                    }
                }

                customForkJoinPool.submit(() -> eventHandler.getActions().parallelStream()
                        .forEach(action -> {
                            eventExecution.setAction(action.getAction());
                            eventExecution.setStatus(Status.IN_PROGRESS);
                            if (executionService.addEventExecution(eventExecution)) {
                                execute(eventExecution, action, payload);
                            } else {
                                logger.warn("Duplicate delivery/execution of message: {}", msg.getId());
                            }
                        })
                ).get();
            }

            // TODO: move to dlq if failed instead of deleting
            queue.ack(Collections.singletonList(msg));
        } catch (Exception e) {
            logger.error("Error handling message: {} on queue:{}", msg, queue.getName(), e);
        }
    }

    @SuppressWarnings("Guava")
    private void execute(EventExecution eventExecution, Action action, String payload) {
        try {
            String methodName = "executeEventAction";
            String description = String.format("Executing action: %s for event: %s with messageId: %s with payload: %s", action.getAction(), eventExecution.getId(), eventExecution.getMessageId(), payload);
            logger.debug(description);

            Predicate<Throwable> filterException = throwableException -> !(throwableException instanceof UnsupportedOperationException);
            Map<String, Object> output = new RetryUtil<Map<String, Object>>().retryOnException(() -> actionProcessor.execute(action, payload, eventExecution.getEvent(), eventExecution.getMessageId()),
                    filterException, null, RETRY_COUNT, description, methodName);

            if (output != null) {
                eventExecution.getOutput().putAll(output);
            }
            eventExecution.setStatus(Status.COMPLETED);
        } catch (RuntimeException e) {
            logger.error("Error executing action: {} for event: {} with messageId: {}", action.getAction(), eventExecution.getEvent(), eventExecution.getMessageId(), e);
            eventExecution.setStatus(Status.FAILED);
            eventExecution.getOutput().put("exception", e.getMessage());
        }
        executionService.updateEventExecution(eventExecution);
    }
}
