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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.metadata.events.EventHandler.Action;
import com.netflix.conductor.common.metadata.events.EventHandler.Action.Type;
import com.netflix.conductor.common.metadata.events.EventHandler.StartWorkflow;
import com.netflix.conductor.common.metadata.events.EventHandler.TaskDetails;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.execution.TestConfiguration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.service.ExecutionService;
import com.netflix.conductor.service.MetadataService;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import rx.Observable;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Viren
 */
public class TestEventProcessor {

    @Test
    public void testEventProcessor() throws Exception {
        String event = "sqs:arn:account090:sqstest1";
        String queueURI = "arn:account090:sqstest1";

        EventQueueProvider provider = mock(EventQueueProvider.class);

        ObservableQueue queue = mock(ObservableQueue.class);
        Message[] messages = new Message[1];
        messages[0] = new Message("t0", "{}", "t0");

        Observable<Message> msgObservable = Observable.from(messages);
        when(queue.observe()).thenReturn(msgObservable);
        when(queue.getURI()).thenReturn(queueURI);
        when(queue.getName()).thenReturn(queueURI);
        when(queue.getType()).thenReturn("sqs");
        when(provider.getQueue(queueURI)).thenReturn(queue);
        EventQueues.providers = new HashMap<>();

        EventQueues.providers.put("sqs", provider);

        // setup event handler
        EventHandler eventHandler = new EventHandler();
        eventHandler.setName(UUID.randomUUID().toString());
        eventHandler.setActive(false);

        Action startWorkflowAction = new Action();
        startWorkflowAction.setAction(Type.start_workflow);
        startWorkflowAction.setStart_workflow(new StartWorkflow());
        startWorkflowAction.getStart_workflow().setName("workflow_x");
        startWorkflowAction.getStart_workflow().setVersion(1);
        eventHandler.getActions().add(startWorkflowAction);

        Action completeTaskAction = new Action();
        completeTaskAction.setAction(Type.complete_task);
        completeTaskAction.setComplete_task(new TaskDetails());
        completeTaskAction.getComplete_task().setTaskRefName("task_x");
        completeTaskAction.getComplete_task().setWorkflowId(UUID.randomUUID().toString());
        completeTaskAction.getComplete_task().setOutput(new HashMap<>());
        eventHandler.getActions().add(completeTaskAction);

        eventHandler.setEvent(event);

        //Metadata Service Mock
        MetadataService metadataService = mock(MetadataService.class);
        when(metadataService.getEventHandlers()).thenReturn(Collections.singletonList(eventHandler));
        when(metadataService.getEventHandlersForEvent(eventHandler.getEvent(), true)).thenReturn(Collections.singletonList(eventHandler));

        //Execution Service Mock
        ExecutionService executionService = mock(ExecutionService.class);
        when(executionService.addEventExecution(any())).thenReturn(true);

        //Workflow Executor Mock
        WorkflowExecutor executor = mock(WorkflowExecutor.class);
        String id = UUID.randomUUID().toString();
        AtomicBoolean started = new AtomicBoolean(false);
        doAnswer((Answer<String>) invocation -> {
            started.set(true);
            return id;
        }).when(executor).startWorkflow(startWorkflowAction.getStart_workflow().getName(), startWorkflowAction.getStart_workflow().getVersion(), startWorkflowAction.getStart_workflow().getCorrelationId(), startWorkflowAction.getStart_workflow().getInput(), event);

        AtomicBoolean completed = new AtomicBoolean(false);
        doAnswer((Answer<String>) invocation -> {
            completed.set(true);
            return null;
        }).when(executor).updateTask(any());

        Task task = new Task();
        task.setReferenceTaskName(completeTaskAction.getComplete_task().getTaskRefName());
        Workflow workflow = new Workflow();
        workflow.setTasks(Collections.singletonList(task));
        when(executor.getWorkflow(completeTaskAction.getComplete_task().getWorkflowId(), true)).thenReturn(workflow);

        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setVersion(startWorkflowAction.getStart_workflow().getVersion());
        workflowDef.setName(startWorkflowAction.getStart_workflow().getName());
        when(metadataService.getWorkflowDef(any(), any())).thenReturn(workflowDef);

        ActionProcessor actionProcessor = new ActionProcessor(executor, metadataService);

        EventProcessor eventProcessor = new EventProcessor(executionService, metadataService, actionProcessor, new TestConfiguration());
        assertNotNull(eventProcessor.getQueues());
        assertEquals(1, eventProcessor.getQueues().size());

        String queueEvent = eventProcessor.getQueues().keySet().iterator().next();
        assertEquals(eventHandler.getEvent(), queueEvent);

        String eventProcessorQueue = eventProcessor.getQueues().values().iterator().next();
        assertEquals(queueURI, eventProcessorQueue);

        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        assertTrue(started.get());
        assertTrue(completed.get());
    }
}
