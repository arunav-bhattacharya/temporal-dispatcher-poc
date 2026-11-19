package com.payments.dispatch.dispatch

import com.payments.dispatch.config.AppConfig
import com.payments.dispatch.config.DispatchMode
import com.payments.dispatch.config.dispatchMode
import com.payments.dispatch.events.EventType
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.Activity
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import jakarta.inject.Singleton
import java.time.Duration

private val log = KotlinLogging.logger {}

data class DispatchResult(
    val eventType: String,
    val engine: String,
    val claimed: Int,
    val started: Int,
    val duplicates: Int,
    val reconciled: Int,
    val stopReason: String,
    val millis: Long,
)

/** One generic workflow type for every event type. Each Temporal Schedule starts it with its event type. */
@WorkflowInterface
interface DispatcherWorkflow {
    @WorkflowMethod
    fun run(eventType: String): DispatchResult
}

class DispatcherWorkflowImpl : DispatcherWorkflow {
    private val activities = Workflow.newActivityStub(
        DispatchActivities::class.java,
        ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofMinutes(10))
            .setHeartbeatTimeout(Duration.ofSeconds(60))
            .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(3).build())
            .build(),
    )

    override fun run(eventType: String): DispatchResult = activities.dispatch(eventType)
}

@ActivityInterface
interface DispatchActivities {
    @ActivityMethod
    fun dispatch(eventType: String): DispatchResult
}

/** The only place the two options differ: which engine runs the claim-and-start loop. */
@Singleton
class DispatchActivitiesImpl(
    config: AppConfig,
    store: com.payments.dispatch.events.PaymentEventRepository,
    starter: TemporalWorkflowStarter,
    private val limitsRepo: DispatchConfigRepository,
    private val reaper: StaleRowReaper,
) : DispatchActivities {

    private val engine: DispatchEngine = when (config.dispatchMode()) {
        DispatchMode.POLLING -> PollingDispatchEngine(store, starter)
        DispatchMode.REACTIVE -> ReactiveDispatchEngine(store, starter)
    }

    override fun dispatch(eventType: String): DispatchResult {
        val type = EventType.valueOf(eventType)
        val context = Activity.getExecutionContext()
        val t0 = System.currentTimeMillis()
        val limits = limitsRepo.load(type)
        if (!limits.enabled) {
            return DispatchResult(eventType, engine.name, 0, 0, 0, 0, StopReason.DISABLED.name, 0)
        }
        val reconciled = reaper.reconcile(type)
        val r = engine.dispatch(type, limits) { progress -> context.heartbeat(progress) }
        val result = DispatchResult(
            eventType, engine.name, r.claimed, r.started, r.duplicates, reconciled, r.stopReason.name,
            System.currentTimeMillis() - t0,
        )
        if (r.claimed > 0 || reconciled > 0) log.info { "dispatch $result" }
        return result
    }
}
