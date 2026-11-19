package com.payments.dispatch.workflows

import com.payments.dispatch.config.Json
import com.payments.dispatch.events.EventRef
import com.payments.dispatch.events.PaymentEventRepository
import com.payments.dispatch.events.ReturnedPayment
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import jakarta.inject.Singleton
import java.math.BigDecimal

private val log = KotlinLogging.logger {}

data class OriginalPayment(val paymentId: String, val accountId: String, val amount: BigDecimal, val postingId: String)

/** PAYMENT_RETURNED: reverse the original posting, apply NACHA-code rules, tell the customer. */
@WorkflowInterface
interface ReturnedPaymentWorkflow {
    @WorkflowMethod
    fun process(ref: EventRef): String
}

class ReturnedPaymentWorkflowImpl : ReturnedPaymentWorkflow {
    private val acts = Workflow.newActivityStub(ReturnedPaymentActivities::class.java, ActivityDefaults.standard())
    private val status = Workflow.newActivityStub(EventStatusActivities::class.java, ActivityDefaults.standard())

    override fun process(ref: EventRef): String = processEvent(ref, status) {
        val ret = acts.load(ref.eventId)
        val original = acts.findOriginal(ret)
        val reversal = acts.reversePosting(original, ret)
        val actions = mutableListOf("reversed $reversal")
        if (ret.returnCode in FEE_CODES) actions += "fee ${acts.chargeReturnFee(ret.accountId, RETURN_FEE, ret.returnCode)}"
        if (ret.returnCode in BLOCK_CODES) {
            acts.blockAchDebits(ret.accountId, ret.returnCode)
            actions += "ACH debits blocked"
        }
        acts.notifyCustomer(ret.accountId, "Payment ${ret.paymentId} was returned: ${ret.returnReason} (${ret.returnCode})")
        "RETURNED ${ret.returnCode}: " + actions.joinToString("; ")
    }

    companion object {
        /** Insufficient / uncollected funds: the customer pays a return fee. */
        val FEE_CODES = setOf("R01", "R09")

        /** Account closed / not found / invalid: stop further ACH debits to it. */
        val BLOCK_CODES = setOf("R02", "R03", "R04")
        val RETURN_FEE = BigDecimal("25.00")
    }
}

@ActivityInterface(namePrefix = "Returned")
interface ReturnedPaymentActivities {
    @ActivityMethod fun load(eventId: Long): ReturnedPayment
    @ActivityMethod fun findOriginal(ret: ReturnedPayment): OriginalPayment
    @ActivityMethod fun reversePosting(original: OriginalPayment, ret: ReturnedPayment): String
    @ActivityMethod fun chargeReturnFee(accountId: String, fee: BigDecimal, returnCode: String): String
    @ActivityMethod fun blockAchDebits(accountId: String, returnCode: String)
    @ActivityMethod fun notifyCustomer(accountId: String, message: String)
}

@Singleton
class ReturnedPaymentActivitiesImpl(
    private val repo: PaymentEventRepository,
    private val sim: Simulator,
) : ReturnedPaymentActivities {
    override fun load(eventId: Long): ReturnedPayment = Json.read(repo.loadPayload(eventId))

    override fun findOriginal(ret: ReturnedPayment): OriginalPayment {
        sim.work()
        return OriginalPayment(ret.paymentId, ret.accountId, ret.amount, sim.id("POST"))
    }

    override fun reversePosting(original: OriginalPayment, ret: ReturnedPayment): String {
        sim.work()
        return sim.id("REV")
    }

    override fun chargeReturnFee(accountId: String, fee: BigDecimal, returnCode: String): String {
        sim.work()
        return sim.id("FEE")
    }

    override fun blockAchDebits(accountId: String, returnCode: String) {
        sim.work()
        log.debug { "ACH debits blocked on $accountId after $returnCode" }
    }

    override fun notifyCustomer(accountId: String, message: String) {
        sim.work()
        log.debug { "notify $accountId: $message" }
    }
}
