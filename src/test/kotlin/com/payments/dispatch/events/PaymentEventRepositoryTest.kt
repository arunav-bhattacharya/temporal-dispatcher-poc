package com.payments.dispatch.events

import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.deleteAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Callable
import java.util.concurrent.Executors

/** Claim semantics against a real Postgres (Quarkus Dev Services starts one in Docker). */
@QuarkusTest
class PaymentEventRepositoryTest {

    @Inject
    lateinit var repo: PaymentEventRepository

    @Inject
    lateinit var generator: SampleEventGenerator

    @Inject
    lateinit var db: Database

    private val type = EventType.PAYMENT_POSTED
    private val lease = Duration.ofMinutes(2)

    @BeforeEach
    fun clean() {
        transaction(db) { PaymentEvents.deleteAll() }
    }

    @Test
    fun `payload round-trips through jsonb`() {
        repo.insertAll(generator.generate(EventType.INBOUND_PAYMENT, 1))
        val claimed = repo.claim(EventType.INBOUND_PAYMENT, 10, lease).single()
        val payload = com.payments.dispatch.config.Json.read<InboundPayment>(repo.loadPayload(claimed.id))
        assertTrue(payload.inboundId.startsWith("INB-"))
    }

    @Test
    fun `concurrent claims never return the same row (SKIP LOCKED)`() {
        repo.insertAll(generator.generate(type, 500))
        val pool = Executors.newFixedThreadPool(8)
        val results = pool.invokeAll(List(8) { Callable { repo.claim(type, 100, lease).map { it.id } } }).map { it.get() }
        pool.shutdown()
        val all = results.flatten()
        assertEquals(all.size, all.toSet().size, "a row was claimed twice")
        assertEquals(500, all.size)
        assertEquals(500, repo.countInflight(type))
    }

    @Test
    fun `rows that are not yet due are not claimable`() {
        repo.insertAll(listOf(NewEvent(EventType.PAYMENT_SCHEDULED, "{}", Instant.now().plus(Duration.ofHours(1)))))
        repo.insertAll(listOf(NewEvent(EventType.PAYMENT_SCHEDULED, "{}", Instant.now().minusSeconds(1))))
        assertEquals(1, repo.claim(EventType.PAYMENT_SCHEDULED, 10, lease).size)
    }

    @Test
    fun `an expired lease makes a CLAIMED row claimable again, a STARTED row is not`() {
        repo.insertAll(generator.generate(type, 2))
        val (a, b) = repo.claim(type, 2, lease)
        repo.markStarted(a.id, type.workflowId(a.id))
        Thread.sleep(1_100)
        val reclaimed = repo.claim(type, 10, Duration.ofSeconds(1))
        assertEquals(listOf(b.id), reclaimed.map { it.id })
    }

    @Test
    fun `release returns claimed rows to NEW`() {
        repo.insertAll(generator.generate(type, 3))
        val ids = repo.claim(type, 3, lease).map { it.id }
        repo.release(ids)
        assertEquals(0, repo.countInflight(type))
        assertEquals(3, repo.claim(type, 10, lease).size)
    }

    @Test
    fun `done and failed rows leave the in-flight count`() {
        repo.insertAll(generator.generate(type, 3))
        val (a, b, _) = repo.claim(type, 3, lease)
        repo.markStarted(a.id, "w-a")
        repo.markDone(a.id, "ok")
        repo.markFailed(b.id, "boom")
        assertEquals(1, repo.countInflight(type))
        assertEquals(EventStatus.DONE, repo.find(a.id)!!.status)
        assertEquals("boom", repo.find(b.id)!!.lastError)
    }
}
