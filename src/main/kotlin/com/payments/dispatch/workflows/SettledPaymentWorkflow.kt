package com.payments.dispatch.workflows

import com.payments.dispatch.config.Json
import com.payments.dispatch.events.EventRef
import com.payments.dispatch.events.PaymentEventRepository
import com.payments.dispatch.events.SettledPayment
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import jakarta.inject.Singleton
import java.math.BigDecimal

data class Reconciliation(val matched: Boolean, val expectedFee: BigDecimal, val actualFee: BigDecimal)

/** PAYMENT_SETTLED: reconcile against what we expected, release the funds hold, close the payment. */
@WorkflowInterface
interface SettledPaymentWorkflow {
    @WorkflowMethod
    fun process(ref: EventRef): String
}

class SettledPaymentWorkflowImpl : SettledPaymentWorkflow {
    private val acts = Workflow.newActivityStub(SettledPaymentActivities::class.java, ActivityDefaults.standard())
    private val status = Workflow.newActivityStub(EventStatusActivities::class.java, ActivityDefaults.standard())

    override fun process(ref: EventRef): String = processEvent(ref, status) {
        val settlement = acts.load(ref.eventId)
        val recon = acts.reconcile(settlement)
        if (!recon.matched) {
            throw businessFailure(
                "Fee mismatch on ${settlement.settlementId}: expected ${recon.expectedFee}, got ${recon.actualFee}",
                "ReconciliationBreak",
            )
        }
        acts.releaseHold(settlement)
        acts.closePayment(settlement)
        "SETTLED ${settlement.paymentId} in ${settlement.clearingSystem} batch ${settlement.batchId}"
    }
}

@ActivityInterface(namePrefix = "Settled")
interface SettledPaymentActivities {
    @ActivityMethod fun load(eventId: Long): SettledPayment
    @ActivityMethod fun reconcile(settlement: SettledPayment): Reconciliation
    @ActivityMethod fun releaseHold(settlement: SettledPayment)
    @ActivityMethod fun closePayment(settlement: SettledPayment)
}

@Singleton
class SettledPaymentActivitiesImpl(
    private val repo: PaymentEventRepository,
    private val sim: Simulator,
) : SettledPaymentActivities {
    override fun load(eventId: Long): SettledPayment = Json.read(repo.loadPayload(eventId))

    /** Fee schedule stand-in: clearing fees above 5.00 don't match what we booked. */
    override fun reconcile(settlement: SettledPayment): Reconciliation {
        sim.work()
        val expected = if (settlement.fee > BigDecimal("5.00")) BigDecimal("0.50") else settlement.fee
        return Reconciliation(expected.compareTo(settlement.fee) == 0, expected, settlement.fee)
    }

    override fun releaseHold(settlement: SettledPayment) = sim.work()

    override fun closePayment(settlement: SettledPayment) = sim.work()
}
