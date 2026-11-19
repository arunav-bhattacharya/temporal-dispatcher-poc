package com.payments.dispatch.dispatch

import com.payments.dispatch.events.EventRef
import io.temporal.api.enums.v1.WorkflowIdReusePolicy
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowExecutionAlreadyStarted
import io.temporal.client.WorkflowOptions
import jakarta.inject.Singleton

/**
 * Starts the business workflow for an event through an untyped stub, so the dispatcher needs no
 * compile-time link to the five workflow interfaces. The workflow ID is derived from the event ID
 * and duplicates are rejected: a retried claim can never start the same event twice.
 */
@Singleton
class TemporalWorkflowStarter(private val client: WorkflowClient) : WorkflowStarter {

    override fun start(event: ClaimedEvent): StartOutcome {
        val options = WorkflowOptions.newBuilder()
            .setWorkflowId(event.type.workflowId(event.id))
            .setTaskQueue(event.type.taskQueue)
            .setWorkflowIdReusePolicy(WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
            .build()
        return try {
            client.newUntypedWorkflowStub(event.type.workflowType, options).start(EventRef(event.id, event.type.name))
            StartOutcome.STARTED
        } catch (_: WorkflowExecutionAlreadyStarted) {
            StartOutcome.DUPLICATE
        }
    }
}
