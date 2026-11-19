package com.payments.dispatch.events

/**
 * The five payment event types and the workflow each one triggers.
 * Each type has its own Temporal Schedule, task queue and dispatch limits.
 */
enum class EventType(val workflowType: String, val taskQueue: String, val payloadClass: Class<*>) {
    INBOUND_PAYMENT("InboundPaymentWorkflow", "inbound", InboundPayment::class.java),
    PAYMENT_RETURNED("ReturnedPaymentWorkflow", "returned", ReturnedPayment::class.java),
    PAYMENT_SCHEDULED("ScheduledPaymentWorkflow", "scheduled", ScheduledPayment::class.java),
    PAYMENT_POSTED("PostedPaymentWorkflow", "posted", PostedPayment::class.java),
    PAYMENT_SETTLED("SettledPaymentWorkflow", "settled", SettledPayment::class.java);

    val scheduleId: String get() = "dispatch-$name"

    /** Deterministic workflow ID: a second start for the same event is rejected by Temporal. */
    fun workflowId(eventId: Long): String = "$name-$eventId"
}

enum class EventStatus { NEW, CLAIMED, STARTED, DONE, FAILED }

/** What the dispatcher hands to every business workflow: an ID, never the payload. */
data class EventRef(val eventId: Long, val eventType: String)
