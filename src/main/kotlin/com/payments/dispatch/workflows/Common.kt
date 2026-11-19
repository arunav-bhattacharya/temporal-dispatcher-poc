package com.payments.dispatch.workflows

import com.payments.dispatch.config.AppConfig
import com.payments.dispatch.events.EventRef
import com.payments.dispatch.events.PaymentEventRepository
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.failure.ActivityFailure
import io.temporal.failure.ApplicationFailure
import io.temporal.failure.TemporalFailure
import jakarta.inject.Singleton
import java.time.Duration
import kotlin.random.Random

/** Final status write shared by every business workflow: the row goes DONE or FAILED. */
@ActivityInterface(namePrefix = "Event")
interface EventStatusActivities {
    @ActivityMethod
    fun markDone(eventId: Long, outcome: String)

    @ActivityMethod
    fun markFailed(eventId: Long, reason: String)
}

@Singleton
class EventStatusActivitiesImpl(private val repo: PaymentEventRepository) : EventStatusActivities {
    override fun markDone(eventId: Long, outcome: String) = repo.markDone(eventId, outcome)
    override fun markFailed(eventId: Long, reason: String) = repo.markFailed(eventId, reason)
}

object ActivityDefaults {
    /** Business activities: retried with backoff; non-retryable ApplicationFailures end the workflow. */
    fun standard(): ActivityOptions = ActivityOptions.newBuilder()
        .setStartToCloseTimeout(Duration.ofSeconds(30))
        .setRetryOptions(
            RetryOptions.newBuilder()
                .setInitialInterval(Duration.ofSeconds(1))
                .setBackoffCoefficient(2.0)
                .setMaximumAttempts(5)
                .build(),
        )
        .build()
}

/**
 * Runs a workflow body and records the outcome on the event row. A business failure
 * (sanctions hit, insufficient funds, …) marks the row FAILED and completes the workflow,
 * so Temporal doesn't retry a decision that was already made.
 */
inline fun processEvent(ref: EventRef, status: EventStatusActivities, body: () -> String): String =
    try {
        val outcome = body()
        status.markDone(ref.eventId, outcome)
        outcome
    } catch (e: TemporalFailure) {
        val reason = reasonOf(e)
        status.markFailed(ref.eventId, reason)
        "FAILED: $reason"
    }

fun reasonOf(e: Throwable): String {
    var cause: Throwable = e
    while (cause is ActivityFailure && cause.cause != null) cause = cause.cause!!
    return (cause as? ApplicationFailure)?.originalMessage ?: cause.message ?: cause.toString()
}

fun businessFailure(message: String, type: String): ApplicationFailure =
    ApplicationFailure.newNonRetryableFailure(message, type)

/** Stands in for the bank's core, screening service and payment rails. */
@Singleton
class Simulator(private val config: AppConfig) {
    fun work() {
        val ms = config.simulation().activityDelay().toMillis()
        if (ms > 0) Thread.sleep(ms + Random.nextLong(ms / 2 + 1))
    }

    fun id(prefix: String): String = prefix + "-" + Random.nextLong(1_000_000_000L, 9_999_999_999L)
}

object Watchlist {
    val names = setOf("Northwind Shell Holdings", "Blue Lantern Trading FZE", "Meridian Offshore Ventures")
}
