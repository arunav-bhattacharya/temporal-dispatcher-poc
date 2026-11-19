package com.payments.dispatch.config

import com.payments.dispatch.dispatch.DispatchActivitiesImpl
import com.payments.dispatch.dispatch.DispatcherWorkflowImpl
import com.payments.dispatch.events.EventType
import com.payments.dispatch.workflows.EventStatusActivitiesImpl
import com.payments.dispatch.workflows.InboundPaymentActivitiesImpl
import com.payments.dispatch.workflows.InboundPaymentWorkflowImpl
import com.payments.dispatch.workflows.PostedPaymentActivitiesImpl
import com.payments.dispatch.workflows.PostedPaymentWorkflowImpl
import com.payments.dispatch.workflows.ReturnedPaymentActivitiesImpl
import com.payments.dispatch.workflows.ReturnedPaymentWorkflowImpl
import com.payments.dispatch.workflows.ScheduledPaymentActivitiesImpl
import com.payments.dispatch.workflows.ScheduledPaymentWorkflowImpl
import com.payments.dispatch.workflows.SettledPaymentActivitiesImpl
import com.payments.dispatch.workflows.SettledPaymentWorkflowImpl
import io.github.oshai.kotlinlogging.KotlinLogging
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import io.temporal.client.WorkflowClient
import io.temporal.worker.WorkerFactory
import io.temporal.worker.WorkerOptions
import jakarta.annotation.Priority
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import java.util.concurrent.TimeUnit

private val log = KotlinLogging.logger {}

/**
 * Six workers in one app: the dispatcher queue plus one task queue per event type, so each
 * workflow type gets its own concurrency limits and a slow type can't starve the others.
 */
@ApplicationScoped
class WorkerSetup(
    private val config: AppConfig,
    private val client: WorkflowClient,
    private val dispatch: DispatchActivitiesImpl,
    private val status: EventStatusActivitiesImpl,
    private val inbound: InboundPaymentActivitiesImpl,
    private val returned: ReturnedPaymentActivitiesImpl,
    private val scheduled: ScheduledPaymentActivitiesImpl,
    private val posted: PostedPaymentActivitiesImpl,
    private val settled: SettledPaymentActivitiesImpl,
) {
    private var factory: WorkerFactory? = null

    fun onStart(@Observes @Priority(10) event: StartupEvent) {
        if (!config.workers().enabled()) return
        val f = WorkerFactory.newInstance(client)
        val options = WorkerOptions.newBuilder()
            .setMaxConcurrentActivityExecutionSize(config.workers().maxConcurrentActivities())
            .setMaxConcurrentWorkflowTaskExecutionSize(config.workers().maxConcurrentWorkflowTasks())
            .build()

        f.newWorker(config.taskQueue(), options).apply {
            registerWorkflowImplementationTypes(DispatcherWorkflowImpl::class.java)
            registerActivitiesImplementations(dispatch)
        }
        val business = listOf(
            Triple(EventType.INBOUND_PAYMENT, InboundPaymentWorkflowImpl::class.java, inbound),
            Triple(EventType.PAYMENT_RETURNED, ReturnedPaymentWorkflowImpl::class.java, returned),
            Triple(EventType.PAYMENT_SCHEDULED, ScheduledPaymentWorkflowImpl::class.java, scheduled),
            Triple(EventType.PAYMENT_POSTED, PostedPaymentWorkflowImpl::class.java, posted),
            Triple(EventType.PAYMENT_SETTLED, SettledPaymentWorkflowImpl::class.java, settled),
        )
        business.forEach { (type, workflowImpl, activities) ->
            f.newWorker(type.taskQueue, options).apply {
                registerWorkflowImplementationTypes(workflowImpl)
                registerActivitiesImplementations(activities, status)
            }
        }
        f.start()
        factory = f
        log.info { "Temporal workers started (mode=${config.dispatchMode()}, queues=${listOf(config.taskQueue()) + business.map { it.first.taskQueue }})" }
    }

    fun onStop(@Observes event: ShutdownEvent) {
        factory?.let {
            it.shutdown()
            it.awaitTermination(10, TimeUnit.SECONDS)
        }
    }
}
