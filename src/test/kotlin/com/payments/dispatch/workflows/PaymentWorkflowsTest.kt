package com.payments.dispatch.workflows

import com.payments.dispatch.config.Json
import com.payments.dispatch.events.EventRef
import com.payments.dispatch.events.EventType
import com.payments.dispatch.events.InboundPayment
import com.payments.dispatch.events.Party
import com.payments.dispatch.events.Rail
import com.payments.dispatch.events.ReturnedPayment
import com.payments.dispatch.events.ScheduledPayment
import io.temporal.client.WorkflowClientOptions
import io.temporal.client.WorkflowOptions
import io.temporal.testing.TestEnvironmentOptions
import io.temporal.testing.TestWorkflowEnvironment
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.Collections

/** Workflow logic against Temporal's in-memory, time-skipping test server. Activities are fakes. */
class PaymentWorkflowsTest {
    private lateinit var env: TestWorkflowEnvironment
    private val status = RecordingStatus()

    @BeforeEach
    fun setUp() {
        env = TestWorkflowEnvironment.newInstance(
            TestEnvironmentOptions.newBuilder()
                .setWorkflowClientOptions(WorkflowClientOptions.newBuilder().setDataConverter(Json.temporalDataConverter).build())
                .build(),
        )
    }

    @AfterEach
    fun tearDown() = env.close()

    @Test
    fun `inbound payment from a clean originator is credited`() {
        val result = runInbound(originator = "Acme Industrial Supply")
        assertTrue(result.startsWith("CREDITED 1250.00 USD to ACC-12345678"), result)
        assertEquals(listOf("done:1"), status.calls)
    }

    @Test
    fun `inbound payment from a watchlisted originator fails the event, not the workflow`() {
        val result = runInbound(originator = Watchlist.names.first())
        assertTrue(result.startsWith("FAILED: Sanctions screening hit"), result)
        assertEquals("failed:1", status.calls.single().substringBefore(" "))
    }

    @Test
    fun `R01 return reverses the posting and charges a fee, R02 blocks ACH debits`() {
        val acts = FakeReturned()
        env.newWorker(EventType.PAYMENT_RETURNED.taskQueue).apply {
            registerWorkflowImplementationTypes(ReturnedPaymentWorkflowImpl::class.java)
            registerActivitiesImplementations(acts, status)
        }
        env.start()
        val r01 = stub<ReturnedPaymentWorkflow>(EventType.PAYMENT_RETURNED, "r01").process(EventRef(1, "R01"))
        val r02 = stub<ReturnedPaymentWorkflow>(EventType.PAYMENT_RETURNED, "r02").process(EventRef(2, "R02"))
        assertTrue(r01.contains("fee FEE-1") && !r01.contains("blocked"), r01)
        assertTrue(r02.contains("ACH debits blocked") && !r02.contains("fee"), r02)
        assertEquals(listOf("ACC-2"), acts.blocked)
    }

    @Test
    fun `scheduled payment sleeps on a durable timer until its due date, then submits`() {
        val dueIn = Duration.ofDays(3)
        val acts = FakeScheduled(scheduledFor = Instant.ofEpochMilli(env.currentTimeMillis()).plus(dueIn))
        env.newWorker(EventType.PAYMENT_SCHEDULED.taskQueue).apply {
            registerWorkflowImplementationTypes(ScheduledPaymentWorkflowImpl::class.java)
            registerActivitiesImplementations(acts, status)
        }
        env.start()
        val before = env.currentTimeMillis()
        val result = stub<ScheduledPaymentWorkflow>(EventType.PAYMENT_SCHEDULED, "sch").process(EventRef(9, EventType.PAYMENT_SCHEDULED.name))
        assertTrue(result.startsWith("SUBMITTED 250.00 USD via ACH"), result)
        assertTrue(env.currentTimeMillis() - before >= dueIn.toMillis(), "test clock should have skipped ~3 days")
        assertTrue(acts.submittedAt!! >= acts.scheduledFor.toEpochMilli(), "submitted before the due date")
    }

    private fun runInbound(originator: String): String {
        env.newWorker(EventType.INBOUND_PAYMENT.taskQueue).apply {
            registerWorkflowImplementationTypes(InboundPaymentWorkflowImpl::class.java)
            registerActivitiesImplementations(FakeInbound(originator), status)
        }
        env.start()
        return stub<InboundPaymentWorkflow>(EventType.INBOUND_PAYMENT, "inb").process(EventRef(1, EventType.INBOUND_PAYMENT.name))
    }

    private inline fun <reified T> stub(type: EventType, id: String): T =
        env.workflowClient.newWorkflowStub(T::class.java, WorkflowOptions.newBuilder().setTaskQueue(type.taskQueue).setWorkflowId(id).build())

    // ---- fakes ---------------------------------------------------------------------------------------

    class RecordingStatus : EventStatusActivities {
        val calls: MutableList<String> = Collections.synchronizedList(mutableListOf())
        override fun markDone(eventId: Long, outcome: String) { calls += "done:$eventId" }
        override fun markFailed(eventId: Long, reason: String) { calls += "failed:$eventId $reason" }
    }

    class FakeInbound(private val originator: String) : InboundPaymentActivities {
        override fun load(eventId: Long) = InboundPayment(
            "INB-1", Rail.WIRE, BigDecimal("1250.00"), "USD", Party(originator, "0001112223", bankBic = "DEUTDEFF"),
            "ACC-12345678", "Invoice INV-2026-00042", "20261119ABCDFT01000123", Instant.EPOCH,
        )
        override fun screen(payment: InboundPayment) =
            ScreeningResult(payment.originator.name in Watchlist.names, payment.originator.name)
        override fun matchAccount(payment: InboundPayment) = AccountMatch(payment.beneficiaryAccountId, "CUST-1")
        override fun credit(payment: InboundPayment, account: AccountMatch) = "LE-1"
        override fun notifyCustomer(account: AccountMatch, message: String) {}
    }

    class FakeReturned : ReturnedPaymentActivities {
        val blocked: MutableList<String> = Collections.synchronizedList(mutableListOf())
        override fun load(eventId: Long): ReturnedPayment {
            val code = if (eventId == 1L) "R01" to "Insufficient funds" else "R02" to "Account closed"
            return ReturnedPayment("PMT-$eventId", "021000021000001", code.first, code.second, BigDecimal("80.00"), "USD", "ACC-$eventId", Rail.ACH, Instant.EPOCH)
        }
        override fun findOriginal(ret: ReturnedPayment) = OriginalPayment(ret.paymentId, ret.accountId, ret.amount, "POST-1")
        override fun reversePosting(original: OriginalPayment, ret: ReturnedPayment) = "REV-1"
        override fun chargeReturnFee(accountId: String, fee: BigDecimal, returnCode: String) = "FEE-1"
        override fun blockAchDebits(accountId: String, returnCode: String) { blocked += accountId }
        override fun notifyCustomer(accountId: String, message: String) {}
    }

    class FakeScheduled(val scheduledFor: Instant) : ScheduledPaymentActivities {
        @Volatile var submittedAt: Long? = null
        override fun load(eventId: Long) = ScheduledPayment(
            "SCH-1", "ACC-1", Party("Oakridge Apartments", "000123456789", "021000021"), BigDecimal("250.00"), "USD",
            Rail.ACH, scheduledFor, "MONTHLY", "Rent", "mobile-app", "idem-1",
        )
        override fun validate(payment: ScheduledPayment) {}
        override fun hasFunds(payment: ScheduledPayment) = true
        override fun submitToRail(payment: ScheduledPayment): String {
            submittedAt = io.temporal.activity.Activity.getExecutionContext().info.scheduledTimestamp
            return "ACH-1"
        }
    }
}
