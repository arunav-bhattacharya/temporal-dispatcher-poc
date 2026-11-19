package com.payments.dispatch.dispatch

import com.payments.dispatch.events.EventType
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.time.Clock
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.min

/**
 * Option 2: Temporal Schedules WITH reactive streams (the in-activity half; the doorbell is the other half).
 *
 * The run is a cold Flow pipeline:
 *
 *   claimedBatches ──flatMapConcat──▶ events ──flatMapMerge(parallelism){ rateLimit; start; markStarted }──▶ collect
 *
 * Backpressure is demand-driven: `emit(batch)` suspends until the start stage has accepted the
 * previous batch, so the next claim only happens when there is room. At most `parallelism` start
 * calls are in flight; when Temporal slows down, those slots stay busy and claiming slows by itself.
 * Throughput ceiling per event type ≈ min(rate limit, parallelism / start latency, in-flight cap / duration).
 *
 * Temporal activities are blocking, so the pipeline runs inside runBlocking on the activity thread;
 * blocking JDBC / gRPC calls hop to a bounded slice of Dispatchers.IO.
 */
@OptIn(ExperimentalCoroutinesApi::class, FlowPreview::class)
class ReactiveDispatchEngine(
    private val store: EventStore,
    private val starter: WorkflowStarter,
    private val clock: Clock = Clock.systemUTC(),
) : DispatchEngine {

    override val name = "reactive"

    override fun dispatch(type: EventType, limits: DispatchLimits, heartbeat: (Int) -> Unit): EngineResult = runBlocking {
        val io = Dispatchers.IO.limitedParallelism(limits.parallelism + 1)
        val bucket = TokenBucket(limits.startsPerSecond)
        val deadline = clock.instant().plus(limits.timeBudget)
        val unstarted = ConcurrentHashMap.newKeySet<Long>()
        var claimed = 0
        var started = 0
        var duplicates = 0
        var stop = StopReason.EMPTY

        val batches: Flow<List<ClaimedEvent>> = flow {
            while (true) {
                if (!clock.instant().isBefore(deadline)) { stop = StopReason.TIME_BUDGET; break }
                val capacity = limits.maxInflight - withContext(io) { store.countInflight(type) }
                if (capacity <= 0) {
                    // At the cap: wait (suspended, no thread held) for workflows to finish, within the budget.
                    if (clock.instant().plus(CAPACITY_POLL).isBefore(deadline)) {
                        delay(CAPACITY_POLL.toMillis())
                        continue
                    }
                    stop = StopReason.INFLIGHT_CAP
                    break
                }
                val batch = withContext(io) { store.claim(type, min(capacity, limits.batchSize), limits.lease) }
                if (batch.isEmpty()) { stop = StopReason.EMPTY; break }
                claimed += batch.size
                batch.forEach { unstarted.add(it.id) }
                emit(batch) // suspends until downstream has room: this is the backpressure
            }
        }

        try {
            batches
                .flatMapConcat { it.asFlow() }
                .flatMapMerge(concurrency = limits.parallelism) { event ->
                    flow {
                        bucket.acquire() // suspends, holds no thread
                        val outcome = withContext(io) {
                            starter.start(event).also { store.markStarted(event.id, type.workflowId(event.id)) }
                        }
                        unstarted.remove(event.id)
                        emit(outcome)
                    }
                }
                .collect { outcome ->
                    if (outcome == StartOutcome.STARTED) started++ else duplicates++
                    if ((started + duplicates) % 25 == 0) heartbeat(started + duplicates)
                }
        } catch (e: Throwable) {
            withContext(NonCancellable + io) { store.release(unstarted.toList()) }
            throw e
        }
        heartbeat(started + duplicates)
        EngineResult(claimed, started, duplicates, stop)
    }
}
