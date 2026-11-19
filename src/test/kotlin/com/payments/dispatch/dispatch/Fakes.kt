package com.payments.dispatch.dispatch

import com.payments.dispatch.events.EventType
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

/** In-memory events table: NEW ids, CLAIMED ids, STARTED ids, plus pre-existing open workflows. */
class FakeEventStore(newRows: Int, private val alreadyInflight: Int = 0) : EventStore {
    private val pending = ArrayDeque((1L..newRows.toLong()).toList())
    val claimed = linkedSetOf<Long>()
    val started = linkedSetOf<Long>()
    val released = mutableListOf<Long>()
    var claimCalls = 0
        private set

    /** Largest number of claimed-but-not-started rows seen at the moment a new claim was made. */
    var maxUnstartedAtClaim = 0
        private set

    @Synchronized
    override fun countInflight(type: EventType): Int = alreadyInflight + claimed.size + started.size

    @Synchronized
    override fun claim(type: EventType, limit: Int, lease: Duration): List<ClaimedEvent> {
        claimCalls++
        maxUnstartedAtClaim = maxOf(maxUnstartedAtClaim, claimed.size)
        val out = mutableListOf<ClaimedEvent>()
        while (out.size < limit && pending.isNotEmpty()) {
            val id = pending.removeFirst()
            claimed += id
            out += ClaimedEvent(id, type)
        }
        return out
    }

    @Synchronized
    override fun markStarted(id: Long, workflowId: String) {
        if (claimed.remove(id)) started += id
    }

    @Synchronized
    override fun release(ids: Collection<Long>) {
        ids.forEach { if (claimed.remove(it)) { released += it; pending.addFirst(it) } }
    }
}

/** Records how many start calls overlap; optionally slow, duplicate or failing. */
class FakeStarter(
    private val latencyMs: Long = 0,
    private val duplicateIds: Set<Long> = emptySet(),
    private val failOnId: Long? = null,
) : WorkflowStarter {
    private val current = AtomicInteger()
    val maxConcurrent = AtomicInteger()
    val calls = AtomicInteger()

    override fun start(event: ClaimedEvent): StartOutcome {
        val now = current.incrementAndGet()
        maxConcurrent.accumulateAndGet(now, ::maxOf)
        try {
            calls.incrementAndGet()
            if (latencyMs > 0) Thread.sleep(latencyMs)
            if (event.id == failOnId) throw IllegalStateException("Temporal unavailable")
            return if (event.id in duplicateIds) StartOutcome.DUPLICATE else StartOutcome.STARTED
        } finally {
            current.decrementAndGet()
        }
    }
}

fun limits(
    batchSize: Int = 10,
    maxInflight: Int = 1_000,
    startsPerSecond: Int = 10_000,
    parallelism: Int = 4,
    timeBudget: Duration = Duration.ofSeconds(30),
) = DispatchLimits(batchSize, maxInflight, startsPerSecond, parallelism, timeBudget, Duration.ofMinutes(2), true)
