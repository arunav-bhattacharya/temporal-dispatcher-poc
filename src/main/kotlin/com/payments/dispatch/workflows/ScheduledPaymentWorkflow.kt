package com.payments.dispatch.workflows

import com.payments.dispatch.config.Json
import com.payments.dispatch.events.EventRef
import com.payments.dispatch.events.PaymentEventRepository
import com.payments.dispatch.events.ScheduledPayment
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import jakarta.inject.Singleton
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant

/**
 * PAYMENT_SCHEDULED: validate now, sleep on a durable timer until the due date, then check funds and submit.
 * The dispatcher only claims the row shortly before it is due (available_at = scheduledFor − lead),
 * so far-future payments never hold an in-flight slot as a sleeping workflow.
 */
@WorkflowInterface
interface ScheduledPaymentWorkflow {
    @WorkflowMethod
    fun process(ref: EventRef): String
}

class ScheduledPaymentWorkflowImpl : ScheduledPaymentWorkflow {
    private val acts = Workflow.newActivityStub(ScheduledPaymentActivities::class.java, ActivityDefaults.standard())
    private val status = Workflow.newActivityStub(EventStatusActivities::class.java, ActivityDefaults.standard())

    override fun process(ref: EventRef): String = processEvent(ref, status) {
        val payment = acts.load(ref.eventId)
        acts.validate(payment)

        val now = Instant.ofEpochMilli(Workflow.currentTimeMillis())
        val wait = Duration.between(now, payment.scheduledFor)
        if (!wait.isNegative && !wait.isZero) Workflow.sleep(wait)

        if (!acts.hasFunds(payment)) {
            throw businessFailure("Insufficient funds in ${payment.debtorAccountId} for ${payment.amount} ${payment.currency}", "InsufficientFunds")
        }
        val trace = acts.submitToRail(payment)
        "SUBMITTED ${payment.amount} ${payment.currency} via ${payment.rail} to ${payment.creditor.name} ($trace)"
    }
}

@ActivityInterface(namePrefix = "Scheduled")
interface ScheduledPaymentActivities {
    @ActivityMethod fun load(eventId: Long): ScheduledPayment
    @ActivityMethod fun validate(payment: ScheduledPayment)
    @ActivityMethod fun hasFunds(payment: ScheduledPayment): Boolean
    @ActivityMethod fun submitToRail(payment: ScheduledPayment): String
}

@Singleton
class ScheduledPaymentActivitiesImpl(
    private val repo: PaymentEventRepository,
    private val sim: Simulator,
) : ScheduledPaymentActivities {
    override fun load(eventId: Long): ScheduledPayment = Json.read(repo.loadPayload(eventId))

    override fun validate(payment: ScheduledPayment) {
        sim.work()
        if (payment.amount <= BigDecimal.ZERO) throw businessFailure("Amount must be positive", "InvalidInstruction")
        if (payment.creditor.accountNumber.isBlank()) throw businessFailure("Creditor account missing", "InvalidInstruction")
    }

    /** The PoC's stand-in for an available-balance check: anything above 20,000 bounces. */
    override fun hasFunds(payment: ScheduledPayment): Boolean {
        sim.work()
        return payment.amount <= BigDecimal("20000")
    }

    override fun submitToRail(payment: ScheduledPayment): String {
        sim.work()
        return sim.id(payment.rail.name)
    }
}
