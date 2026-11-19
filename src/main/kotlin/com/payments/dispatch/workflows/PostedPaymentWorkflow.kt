package com.payments.dispatch.workflows

import com.payments.dispatch.config.Json
import com.payments.dispatch.events.Direction
import com.payments.dispatch.events.EventRef
import com.payments.dispatch.events.PaymentEventRepository
import com.payments.dispatch.events.PostedPayment
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import jakarta.inject.Singleton
import java.math.BigDecimal
import kotlin.random.Random

private val log = KotlinLogging.logger {}

/** PAYMENT_POSTED: project the core posting into the ledger, refresh the available balance, notify. */
@WorkflowInterface
interface PostedPaymentWorkflow {
    @WorkflowMethod
    fun process(ref: EventRef): String
}

class PostedPaymentWorkflowImpl : PostedPaymentWorkflow {
    private val acts = Workflow.newActivityStub(PostedPaymentActivities::class.java, ActivityDefaults.standard())
    private val status = Workflow.newActivityStub(EventStatusActivities::class.java, ActivityDefaults.standard())

    override fun process(ref: EventRef): String = processEvent(ref, status) {
        val posting = acts.load(ref.eventId)
        val entry = acts.writeLedgerEntry(posting)
        val balance = acts.updateAvailableBalance(posting)
        val verb = if (posting.direction == Direction.CREDIT) "credited" else "debited"
        acts.notifyCustomer(posting.accountId, "${posting.amount} ${posting.currency} $verb. Available: $balance")
        "POSTED $entry, available $balance ${posting.currency}"
    }
}

@ActivityInterface(namePrefix = "Posted")
interface PostedPaymentActivities {
    @ActivityMethod fun load(eventId: Long): PostedPayment
    @ActivityMethod fun writeLedgerEntry(posting: PostedPayment): String
    @ActivityMethod fun updateAvailableBalance(posting: PostedPayment): BigDecimal
    @ActivityMethod fun notifyCustomer(accountId: String, message: String)
}

@Singleton
class PostedPaymentActivitiesImpl(
    private val repo: PaymentEventRepository,
    private val sim: Simulator,
) : PostedPaymentActivities {
    override fun load(eventId: Long): PostedPayment = Json.read(repo.loadPayload(eventId))

    override fun writeLedgerEntry(posting: PostedPayment): String {
        sim.work()
        return sim.id("LE")
    }

    override fun updateAvailableBalance(posting: PostedPayment): BigDecimal {
        sim.work()
        return BigDecimal(Random.nextInt(100, 50_000)).setScale(2)
    }

    override fun notifyCustomer(accountId: String, message: String) {
        sim.work()
        log.debug { "notify $accountId: $message" }
    }
}
