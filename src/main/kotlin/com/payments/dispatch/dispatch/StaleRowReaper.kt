package com.payments.dispatch.dispatch

import com.payments.dispatch.config.AppConfig
import com.payments.dispatch.events.EventType
import com.payments.dispatch.events.PaymentEventRepository
import io.github.oshai.kotlinlogging.KotlinLogging
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest
import io.temporal.serviceclient.WorkflowServiceStubs
import jakarta.inject.Singleton

private val log = KotlinLogging.logger {}

/**
 * Repairs rows stuck in STARTED: the workflow ended (terminated, timed out, or finished while the
 * final status write failed) but the row never moved on. Runs at the start of every dispatcher run.
 */
@Singleton
class StaleRowReaper(
    private val repo: PaymentEventRepository,
    private val stubs: WorkflowServiceStubs,
    private val config: AppConfig,
) {
    fun reconcile(type: EventType): Int {
        val stale = repo.staleStarted(type, config.reaper().staleAfter(), config.reaper().batch())
        var fixed = 0
        for (row in stale) {
            val status = try {
                stubs.blockingStub().describeWorkflowExecution(
                    DescribeWorkflowExecutionRequest.newBuilder()
                        .setNamespace(config.temporal().namespace())
                        .setExecution(WorkflowExecution.newBuilder().setWorkflowId(row.workflowId))
                        .build(),
                ).workflowExecutionInfo.status
            } catch (e: StatusRuntimeException) {
                if (e.status.code == Status.Code.NOT_FOUND) null else throw e
            }
            when (status) {
                WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING,
                WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW -> continue
                WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED -> repo.markDone(row.id, "reconciled: workflow completed")
                null -> repo.markFailed(row.id, "reconciled: workflow not found")
                else -> repo.markFailed(row.id, "reconciled: workflow $status")
            }
            fixed++
        }
        if (fixed > 0) log.info { "Reaper fixed $fixed stale $type rows" }
        return fixed
    }
}
