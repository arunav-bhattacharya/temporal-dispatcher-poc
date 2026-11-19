package com.payments.dispatch.dispatch

import com.payments.dispatch.events.EventType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration

/** The same behaviour contract, run against both options' engines. */
abstract class DispatchEngineContract {
    abstract fun engine(store: EventStore, starter: WorkflowStarter): DispatchEngine

    private val type = EventType.INBOUND_PAYMENT

    @Test
    fun `starts every claimable event and stops when the table is empty`() {
        val store = FakeEventStore(newRows = 35)
        val result = engine(store, FakeStarter()).dispatch(type, limits(batchSize = 10))
        assertEquals(35, result.started)
        assertEquals(35, result.claimed)
        assertEquals(StopReason.EMPTY, result.stopReason)
        assertEquals(35, store.started.size)
    }

    @Test
    fun `never claims beyond the in-flight cap`() {
        val store = FakeEventStore(newRows = 100, alreadyInflight = 95)
        val result = engine(store, FakeStarter()).dispatch(type, limits(maxInflight = 100, timeBudget = Duration.ofMillis(1_200)))
        assertEquals(5, result.claimed)
        assertEquals(StopReason.INFLIGHT_CAP, result.stopReason)
    }

    @Test
    fun `counts duplicates as handled, not as failures`() {
        val store = FakeEventStore(newRows = 10)
        val result = engine(store, FakeStarter(duplicateIds = setOf(3L, 7L))).dispatch(type, limits())
        assertEquals(8, result.started)
        assertEquals(2, result.duplicates)
        assertEquals(10, store.started.size)
    }

    @Test
    fun `releases claimed but unstarted rows when a start fails`() {
        val store = FakeEventStore(newRows = 10)
        assertThrows<IllegalStateException> {
            engine(store, FakeStarter(failOnId = 4L)).dispatch(type, limits(batchSize = 10, parallelism = 1))
        }
        assertTrue(store.claimed.isEmpty(), "nothing may stay CLAIMED: ${store.claimed}")
        assertTrue(4L in store.released)
        assertEquals(store.started.size + store.released.size, 10)
    }

    @Test
    fun `respects the time budget`() {
        val store = FakeEventStore(newRows = 1_000)
        val result = engine(store, FakeStarter(latencyMs = 5))
            .dispatch(type, limits(batchSize = 5, parallelism = 1, timeBudget = Duration.ofMillis(200)))
        assertEquals(StopReason.TIME_BUDGET, result.stopReason)
        assertTrue(result.started in 1..999)
    }
}

class DispatchEngineTest {

    @Nested
    inner class Polling : DispatchEngineContract() {
        override fun engine(store: EventStore, starter: WorkflowStarter) = PollingDispatchEngine(store, starter)

        @Test
        fun `starts workflows one at a time`() {
            val starter = FakeStarter(latencyMs = 10)
            PollingDispatchEngine(FakeEventStore(20), starter).dispatch(EventType.PAYMENT_POSTED, limits(parallelism = 8))
            assertEquals(1, starter.maxConcurrent.get())
        }
    }

    @Nested
    inner class Reactive : DispatchEngineContract() {
        override fun engine(store: EventStore, starter: WorkflowStarter) = ReactiveDispatchEngine(store, starter)

        @Test
        fun `keeps at most 'parallelism' start calls in flight`() {
            val starter = FakeStarter(latencyMs = 20)
            ReactiveDispatchEngine(FakeEventStore(60), starter).dispatch(EventType.PAYMENT_POSTED, limits(parallelism = 6))
            assertEquals(60, starter.calls.get())
            assertTrue(starter.maxConcurrent.get() in 2..6, "max concurrent = ${starter.maxConcurrent.get()}")
        }

        @Test
        fun `is faster than one-at-a-time for slow start calls`() {
            fun timeOf(engine: DispatchEngine): Long {
                val t0 = System.nanoTime()
                engine.dispatch(EventType.PAYMENT_POSTED, limits(batchSize = 20, parallelism = 8))
                return (System.nanoTime() - t0) / 1_000_000
            }
            val polling = timeOf(PollingDispatchEngine(FakeEventStore(80), FakeStarter(latencyMs = 15)))
            val reactive = timeOf(ReactiveDispatchEngine(FakeEventStore(80), FakeStarter(latencyMs = 15)))
            assertTrue(reactive * 3 < polling, "reactive=${reactive}ms polling=${polling}ms")
        }

        @Test
        fun `claims the next batch only when the start stage has room`() {
            // Demand-driven: when batch k+1 is claimed, at most `parallelism` rows of batch k are still unstarted.
            val store = FakeEventStore(200)
            ReactiveDispatchEngine(store, FakeStarter(latencyMs = 5)).dispatch(EventType.PAYMENT_POSTED, limits(batchSize = 20, parallelism = 3))
            assertEquals(200, store.started.size)
            assertTrue(store.maxUnstartedAtClaim <= 3, "unstarted rows at claim time = ${store.maxUnstartedAtClaim}")
        }
    }
}
