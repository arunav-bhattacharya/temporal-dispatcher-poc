package com.payments.dispatch.events

import com.payments.dispatch.dispatch.ClaimedEvent
import com.payments.dispatch.dispatch.EventStore
import jakarta.inject.Singleton
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.count
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.inList
import org.jetbrains.exposed.v1.core.less
import org.jetbrains.exposed.v1.core.lessEq
import org.jetbrains.exposed.v1.core.or
import org.jetbrains.exposed.v1.core.plus
import org.jetbrains.exposed.v1.core.vendors.ForUpdateOption
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import java.time.Duration
import java.time.Instant

data class NewEvent(val type: EventType, val payloadJson: String, val availableAt: Instant = Instant.now())

data class StoredEvent(
    val id: Long,
    val type: EventType,
    val status: EventStatus,
    val payloadJson: String,
    val workflowId: String?,
    val outcome: String?,
    val lastError: String?,
)

data class StaleRow(val id: Long, val workflowId: String)

@Singleton
class PaymentEventRepository(private val db: Database) : EventStore {

    private val inflight = listOf(EventStatus.CLAIMED.name, EventStatus.STARTED.name)

    override fun countInflight(type: EventType): Int = transaction(db) {
        PaymentEvents.selectAll()
            .where { (PaymentEvents.eventType eq type.name) and (PaymentEvents.status inList inflight) }
            .count().toInt()
    }

    /**
     * One transaction: lock up to [limit] claimable rows with FOR UPDATE SKIP LOCKED, then flip them to
     * CLAIMED. Concurrent dispatchers (other replicas, a doorbell-triggered run) skip each other's rows.
     */
    override fun claim(type: EventType, limit: Int, lease: Duration): List<ClaimedEvent> {
        if (limit <= 0) return emptyList()
        return transaction(db) {
            val now = Instant.now()
            val leaseExpired = now.minus(lease)
            val ids = PaymentEvents.select(PaymentEvents.id)
                .where {
                    (PaymentEvents.eventType eq type.name) and (
                        ((PaymentEvents.status eq EventStatus.NEW.name) and (PaymentEvents.availableAt lessEq now)) or
                            ((PaymentEvents.status eq EventStatus.CLAIMED.name) and (PaymentEvents.claimedAt less leaseExpired))
                        )
                }
                .orderBy(PaymentEvents.availableAt)
                .limit(limit)
                .forUpdate(ForUpdateOption.PostgreSQL.ForUpdate(ForUpdateOption.PostgreSQL.MODE.SKIP_LOCKED))
                .map { it[PaymentEvents.id].value }
            if (ids.isNotEmpty()) {
                PaymentEvents.update({ PaymentEvents.id inList ids }) {
                    it[status] = EventStatus.CLAIMED.name
                    it[claimedAt] = now
                    it[attempts] = attempts + 1
                }
            }
            ids.map { ClaimedEvent(it, type) }
        }
    }

    override fun markStarted(id: Long, workflowId: String) {
        transaction(db) {
            PaymentEvents.update({ (PaymentEvents.id eq id) and (PaymentEvents.status eq EventStatus.CLAIMED.name) }) {
                it[status] = EventStatus.STARTED.name
                it[startedAt] = Instant.now()
                it[PaymentEvents.workflowId] = workflowId
            }
        }
    }

    override fun release(ids: Collection<Long>) {
        if (ids.isEmpty()) return
        transaction(db) {
            PaymentEvents.update({ (PaymentEvents.id inList ids) and (PaymentEvents.status eq EventStatus.CLAIMED.name) }) {
                it[status] = EventStatus.NEW.name
                it[claimedAt] = null
            }
        }
    }

    fun markDone(id: Long, outcome: String) = finish(id, EventStatus.DONE, outcome, null)

    fun markFailed(id: Long, reason: String) = finish(id, EventStatus.FAILED, null, reason)

    private fun finish(id: Long, to: EventStatus, outcome: String?, error: String?) {
        transaction(db) {
            PaymentEvents.update({ PaymentEvents.id eq id }) {
                it[status] = to.name
                it[finishedAt] = Instant.now()
                it[PaymentEvents.outcome] = outcome
                it[lastError] = error?.take(2000)
            }
        }
    }

    fun loadPayload(id: Long): String = transaction(db) {
        PaymentEvents.select(PaymentEvents.payload).where { PaymentEvents.id eq id }.single()[PaymentEvents.payload]
    }

    fun find(id: Long): StoredEvent? = transaction(db) {
        PaymentEvents.selectAll().where { PaymentEvents.id eq id }.singleOrNull()?.let {
            StoredEvent(
                id = it[PaymentEvents.id].value,
                type = EventType.valueOf(it[PaymentEvents.eventType]),
                status = EventStatus.valueOf(it[PaymentEvents.status]),
                payloadJson = it[PaymentEvents.payload],
                workflowId = it[PaymentEvents.workflowId],
                outcome = it[PaymentEvents.outcome],
                lastError = it[PaymentEvents.lastError],
            )
        }
    }

    fun insertAll(events: List<NewEvent>): Int = transaction(db) {
        PaymentEvents.batchInsert(events, shouldReturnGeneratedValues = false) { e ->
            this[PaymentEvents.eventType] = e.type.name
            this[PaymentEvents.payload] = e.payloadJson
            this[PaymentEvents.status] = EventStatus.NEW.name
            this[PaymentEvents.availableAt] = e.availableAt
            this[PaymentEvents.createdAt] = Instant.now()
            this[PaymentEvents.attempts] = 0
        }
        events.size
    }

    /** STARTED rows that look stuck: the reaper asks Temporal what happened to them. */
    fun staleStarted(type: EventType, olderThan: Duration, limit: Int): List<StaleRow> = transaction(db) {
        PaymentEvents.select(PaymentEvents.id, PaymentEvents.workflowId)
            .where {
                (PaymentEvents.eventType eq type.name) and (PaymentEvents.status eq EventStatus.STARTED.name) and
                    (PaymentEvents.startedAt less Instant.now().minus(olderThan))
            }
            .limit(limit)
            .mapNotNull { row -> row[PaymentEvents.workflowId]?.let { StaleRow(row[PaymentEvents.id].value, it) } }
    }

    /** Row counts per event type and status, for /admin/stats. */
    fun countsByTypeAndStatus(): Map<String, Map<String, Long>> = transaction(db) {
        val n = PaymentEvents.id.count()
        PaymentEvents.select(PaymentEvents.eventType, PaymentEvents.status, n)
            .groupBy(PaymentEvents.eventType, PaymentEvents.status)
            .groupBy({ it[PaymentEvents.eventType] }, { it[PaymentEvents.status] to it[n] })
            .mapValues { (_, pairs) -> pairs.toMap() }
    }
}
