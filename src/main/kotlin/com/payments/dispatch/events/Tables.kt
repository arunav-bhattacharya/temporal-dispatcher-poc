package com.payments.dispatch.events

import org.jetbrains.exposed.v1.core.ColumnType
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.dao.id.LongIdTable
import org.jetbrains.exposed.v1.javatime.timestamp
import org.postgresql.util.PGobject

/** Postgres JSONB column holding raw JSON text. (An Oracle source would map this to a CLOB.) */
class JsonbTextColumnType : ColumnType<String>() {
    override fun sqlType(): String = "JSONB"

    override fun valueFromDB(value: Any): String = when (value) {
        is PGobject -> value.value ?: "null"
        else -> value.toString()
    }

    override fun notNullValueToDB(value: String): Any = PGobject().apply {
        type = "jsonb"
        this.value = value
    }
}

object PaymentEvents : LongIdTable("payment_events") {
    val eventType = varchar("event_type", 40)
    val payload = registerColumn("payload", JsonbTextColumnType())
    val status = varchar("status", 16)
    val availableAt = timestamp("available_at")
    val createdAt = timestamp("created_at")
    val claimedAt = timestamp("claimed_at").nullable()
    val startedAt = timestamp("started_at").nullable()
    val finishedAt = timestamp("finished_at").nullable()
    val attempts = integer("attempts")
    val workflowId = varchar("workflow_id", 80).nullable()
    val outcome = text("outcome").nullable()
    val lastError = text("last_error").nullable()
}

object DispatchConfigTable : Table("dispatch_config") {
    val eventType = varchar("event_type", 40)
    val batchSize = integer("batch_size")
    val maxInflight = integer("max_inflight")
    val startsPerSecond = integer("starts_per_second")
    val parallelism = integer("parallelism")
    val timeBudgetSeconds = integer("time_budget_seconds")
    val leaseSeconds = integer("lease_seconds")
    val enabled = bool("enabled")
    val updatedAt = timestamp("updated_at")

    override val primaryKey = PrimaryKey(eventType)
}
