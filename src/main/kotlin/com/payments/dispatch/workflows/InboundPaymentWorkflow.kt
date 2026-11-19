package com.payments.dispatch.workflows

import com.payments.dispatch.config.Json
import com.payments.dispatch.events.EventRef
import com.payments.dispatch.events.InboundPayment
import com.payments.dispatch.events.PaymentEventRepository
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import jakarta.inject.Singleton

private val log = KotlinLogging.logger {}

data class ScreeningResult(val hit: Boolean, val matchedName: String? = null)

data class AccountMatch(val accountId: String, val customerId: String)

/** INBOUND_PAYMENT: screen the originator, find our customer's account, credit it, tell the customer. */
@WorkflowInterface
interface InboundPaymentWorkflow {
    @WorkflowMethod
    fun process(ref: EventRef): String
}

class InboundPaymentWorkflowImpl : InboundPaymentWorkflow {
    private val acts = Workflow.newActivityStub(InboundPaymentActivities::class.java, ActivityDefaults.standard())
    private val status = Workflow.newActivityStub(EventStatusActivities::class.java, ActivityDefaults.standard())

    override fun process(ref: EventRef): String = processEvent(ref, status) {
        val payment = acts.load(ref.eventId)
        val screening = acts.screen(payment)
        if (screening.hit) {
            throw businessFailure("Sanctions screening hit on '${screening.matchedName}', held for review", "SanctionsHit")
        }
        val account = acts.matchAccount(payment)
        val ledgerEntry = acts.credit(payment, account)
        acts.notifyCustomer(account, "You received ${payment.amount} ${payment.currency} from ${payment.originator.name}")
        "CREDITED ${payment.amount} ${payment.currency} to ${account.accountId} ($ledgerEntry)"
    }
}

@ActivityInterface(namePrefix = "Inbound")
interface InboundPaymentActivities {
    @ActivityMethod fun load(eventId: Long): InboundPayment
    @ActivityMethod fun screen(payment: InboundPayment): ScreeningResult
    @ActivityMethod fun matchAccount(payment: InboundPayment): AccountMatch
    @ActivityMethod fun credit(payment: InboundPayment, account: AccountMatch): String
    @ActivityMethod fun notifyCustomer(account: AccountMatch, message: String)
}

@Singleton
class InboundPaymentActivitiesImpl(
    private val repo: PaymentEventRepository,
    private val sim: Simulator,
) : InboundPaymentActivities {
    override fun load(eventId: Long): InboundPayment = Json.read(repo.loadPayload(eventId))

    override fun screen(payment: InboundPayment): ScreeningResult {
        sim.work()
        val hit = payment.originator.name in Watchlist.names
        return ScreeningResult(hit, if (hit) payment.originator.name else null)
    }

    override fun matchAccount(payment: InboundPayment): AccountMatch {
        sim.work()
        return AccountMatch(payment.beneficiaryAccountId, "CUST-" + payment.beneficiaryAccountId.takeLast(6))
    }

    override fun credit(payment: InboundPayment, account: AccountMatch): String {
        sim.work()
        return sim.id("LE")
    }

    override fun notifyCustomer(account: AccountMatch, message: String) {
        sim.work()
        log.debug { "notify ${account.customerId}: $message" }
    }
}
