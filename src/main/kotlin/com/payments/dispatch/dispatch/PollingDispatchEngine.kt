package com.payments.dispatch.dispatch

import com.payments.dispatch.events.EventType
import java.time.Clock
import kotlin.math.min

/**
 * Option 1: Temporal Schedules WITHOUT reactive streams.
 *
 * A plain loop: claim a batch (never more than the in-flight cap allows), start each workflow
 * one at a time behind a blocking token bucket, mark it STARTED, repeat. Backpressure is
 * structural: the loop only pulls what it can start, and a slow start call slows the loop.
 * Throughput ceiling per event type ≈ min(rate limit, 1 / start latency).
 */
class PollingDispatchEngine(
    private val store: EventStore,
    private val starter: WorkflowStarter,
    private val clock: Clock = Clock.systemUTC(),
) : DispatchEngine {

    override val name = "polling"

    override fun dispatch(type: EventType, limits: DispatchLimits, heartbeat: (Int) -> Unit): EngineResult {
        val bucket = TokenBucket(limits.startsPerSecond)
        val deadline = clock.instant().plus(limits.timeBudget)
        var claimed = 0
        var started = 0
        var duplicates = 0

        while (true) {
            if (!clock.instant().isBefore(deadline)) return EngineResult(claimed, started, duplicates, StopReason.TIME_BUDGET)

            val capacity = limits.maxInflight - store.countInflight(type)
            if (capacity <= 0) {
                // At the cap: wait for workflows to finish rather than giving up until the next tick.
                if (clock.instant().plus(CAPACITY_POLL).isBefore(deadline)) {
                    Thread.sleep(CAPACITY_POLL.toMillis())
                    heartbeat(started + duplicates)
                    continue
                }
                return EngineResult(claimed, started, duplicates, StopReason.INFLIGHT_CAP)
            }

            val batch = store.claim(type, min(capacity, limits.batchSize), limits.lease)
            if (batch.isEmpty()) return EngineResult(claimed, started, duplicates, StopReason.EMPTY)
            claimed += batch.size

            var next = 0
            try {
                while (next < batch.size) {
                    val event = batch[next]
                    bucket.acquireBlocking()
                    when (starter.start(event)) {
                        StartOutcome.STARTED -> started++
                        StartOutcome.DUPLICATE -> duplicates++
                    }
                    store.markStarted(event.id, type.workflowId(event.id))
                    next++
                }
            } catch (e: Exception) {
                // Give back what we claimed but never started, so the next run picks it up immediately.
                store.release(batch.drop(next).map { it.id })
                throw e
            }
            heartbeat(started + duplicates)
        }
    }
}
