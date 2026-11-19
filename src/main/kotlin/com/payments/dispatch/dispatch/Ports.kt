package com.payments.dispatch.dispatch

import com.payments.dispatch.events.EventType
import java.time.Duration

/** A row this dispatcher run owns (status CLAIMED, under lease). */
data class ClaimedEvent(val id: Long, val type: EventType)

/** The slice of the events table a dispatch engine needs. Backed by Exposed in production, a fake in tests. */
interface EventStore {
    /** CLAIMED + STARTED rows for this type: what the in-flight cap is measured against. */
    fun countInflight(type: EventType): Int

    /** Claims up to [limit] NEW rows that are due (or CLAIMED rows whose lease expired) with SKIP LOCKED. */
    fun claim(type: EventType, limit: Int, lease: Duration): List<ClaimedEvent>

    fun markStarted(id: Long, workflowId: String)

    /** Hands claimed-but-not-started rows back to NEW after a failure. */
    fun release(ids: Collection<Long>)
}

enum class StartOutcome { STARTED, DUPLICATE }

/** Starts the business workflow for one event. */
fun interface WorkflowStarter {
    fun start(event: ClaimedEvent): StartOutcome
}

/** Runtime limits for one event type, read from dispatch_config at the start of every run. */
data class DispatchLimits(
    val batchSize: Int,
    val maxInflight: Int,
    val startsPerSecond: Int,
    val parallelism: Int,
    val timeBudget: Duration,
    val lease: Duration,
    val enabled: Boolean,
)

enum class StopReason { EMPTY, INFLIGHT_CAP, TIME_BUDGET, DISABLED }

/** How often a run re-checks capacity while it sits at the in-flight cap. */
val CAPACITY_POLL: Duration = Duration.ofMillis(500)

data class EngineResult(val claimed: Int, val started: Int, val duplicates: Int, val stopReason: StopReason)

/** One dispatcher run for one event type. Both options implement this. */
interface DispatchEngine {
    val name: String
    fun dispatch(type: EventType, limits: DispatchLimits, heartbeat: (Int) -> Unit = {}): EngineResult
}
