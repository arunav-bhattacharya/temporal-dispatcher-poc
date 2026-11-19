package com.payments.dispatch.dispatch

import com.payments.dispatch.events.DispatchConfigTable
import com.payments.dispatch.events.EventType
import jakarta.inject.Singleton
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.Database
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import java.time.Duration
import java.time.Instant

/** Reads and writes the per-type runtime limits in dispatch_config. */
@Singleton
class DispatchConfigRepository(private val db: Database) {

    fun load(type: EventType): DispatchLimits = transaction(db) {
        DispatchConfigTable.selectAll().where { DispatchConfigTable.eventType eq type.name }.single().toLimits()
    }

    fun all(): Map<String, DispatchLimits> = transaction(db) {
        DispatchConfigTable.selectAll().associate { it[DispatchConfigTable.eventType] to it.toLimits() }
    }

    fun update(type: EventType, limits: DispatchLimits): Boolean = transaction(db) {
        DispatchConfigTable.update({ DispatchConfigTable.eventType eq type.name }) {
            it[batchSize] = limits.batchSize
            it[maxInflight] = limits.maxInflight
            it[startsPerSecond] = limits.startsPerSecond
            it[parallelism] = limits.parallelism
            it[timeBudgetSeconds] = limits.timeBudget.seconds.toInt()
            it[leaseSeconds] = limits.lease.seconds.toInt()
            it[enabled] = limits.enabled
            it[updatedAt] = Instant.now()
        } == 1
    }

    private fun ResultRow.toLimits() = DispatchLimits(
        batchSize = this[DispatchConfigTable.batchSize],
        maxInflight = this[DispatchConfigTable.maxInflight],
        startsPerSecond = this[DispatchConfigTable.startsPerSecond],
        parallelism = this[DispatchConfigTable.parallelism],
        timeBudget = Duration.ofSeconds(this[DispatchConfigTable.timeBudgetSeconds].toLong()),
        lease = Duration.ofSeconds(this[DispatchConfigTable.leaseSeconds].toLong()),
        enabled = this[DispatchConfigTable.enabled],
    )
}
