package com.payments.dispatch.events

import com.payments.dispatch.config.AppConfig
import com.payments.dispatch.config.Json
import com.payments.dispatch.workflows.Watchlist
import jakarta.inject.Singleton
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID
import kotlin.random.Random

/** Realistic-looking sample events for the five types. All names, numbers and banks are made up. */
@Singleton
class SampleEventGenerator(private val config: AppConfig) {

    fun generate(type: EventType, count: Int, dueWithin: Duration = Duration.ofMinutes(10)): List<NewEvent> =
        List(count) {
            when (type) {
                EventType.INBOUND_PAYMENT -> NewEvent(type, Json.write(inbound()))
                EventType.PAYMENT_RETURNED -> NewEvent(type, Json.write(returned()))
                EventType.PAYMENT_POSTED -> NewEvent(type, Json.write(posted()))
                EventType.PAYMENT_SETTLED -> NewEvent(type, Json.write(settled()))
                EventType.PAYMENT_SCHEDULED -> {
                    val p = scheduled(dueWithin)
                    // Claimable shortly before it's due, never earlier than now.
                    val available = maxOf(Instant.now(), p.scheduledFor.minus(config.simulation().scheduledLead()))
                    NewEvent(type, Json.write(p), available)
                }
            }
        }

    fun inbound(): InboundPayment {
        val rail = pick(Rail.WIRE to 40, Rail.ACH to 40, Rail.RTP to 20)
        val onWatchlist = Random.nextDouble() < config.simulation().screeningHitRate()
        val name = if (onWatchlist) Watchlist.names.random() else COMPANIES.random()
        return InboundPayment(
            inboundId = "INB-" + digits(10),
            rail = rail,
            amount = amount(rail),
            currency = "USD",
            originator = Party(
                name = name,
                accountNumber = digits(10),
                routingNumber = if (rail == Rail.WIRE) null else ROUTING.random(),
                bankBic = if (rail == Rail.WIRE) BICS.random() else null,
            ),
            beneficiaryAccountId = "ACC-" + digits(8),
            remittanceInfo = "Invoice INV-2026-" + digits(5),
            referenceNumber = when (rail) {
                Rail.WIRE -> today() + letters(4) + "FT" + digits(2) + digits(6) // IMAD-like
                Rail.ACH -> ROUTING.random().take(8) + digits(7) // trace number
                Rail.RTP -> "E2E" + UUID.randomUUID().toString().replace("-", "").take(29)
            },
            receivedAt = Instant.now(),
        )
    }

    fun returned(): ReturnedPayment {
        val (code, reason) = RETURN_CODES.random()
        return ReturnedPayment(
            paymentId = "PMT-" + digits(10),
            originalTraceNumber = ROUTING.random().take(8) + digits(7),
            returnCode = code,
            returnReason = reason,
            amount = money(5.0, 5_000.0),
            currency = "USD",
            accountId = "ACC-" + digits(8),
            rail = Rail.ACH,
            returnedAt = Instant.now(),
        )
    }

    fun scheduled(dueWithin: Duration): ScheduledPayment {
        val (payee, memo) = PAYEES.random()
        val rail = pick(Rail.ACH to 70, Rail.RTP to 20, Rail.WIRE to 10)
        val dueInMs = Random.nextLong(10_000L, maxOf(10_001L, dueWithin.toMillis()))
        return ScheduledPayment(
            paymentId = "SCH-" + digits(10),
            debtorAccountId = "ACC-" + digits(8),
            creditor = Party(payee, digits(12), ROUTING.random()),
            // ~3% above the PoC's 20,000 funds threshold, so some bounce on the due date.
            amount = if (Random.nextInt(100) < 3) money(20_001.0, 40_000.0) else money(25.0, 6_000.0),
            currency = "USD",
            rail = rail,
            scheduledFor = Instant.now().plusMillis(dueInMs),
            frequency = pick("ONE_TIME" to 60, "MONTHLY" to 35, "WEEKLY" to 5),
            memo = memo,
            initiatedBy = pick("mobile-app" to 55, "online-banking" to 40, "branch" to 5),
            idempotencyKey = UUID.randomUUID().toString(),
        )
    }

    fun posted(): PostedPayment = PostedPayment(
        paymentId = "PMT-" + digits(10),
        accountId = "ACC-" + digits(8),
        direction = pick(Direction.DEBIT to 60, Direction.CREDIT to 40),
        amount = money(1.0, 3_000.0),
        currency = "USD",
        postedAt = Instant.now(),
        valueDate = LocalDate.now(ZoneOffset.UTC),
        channel = pick("CARD" to 45, "ACH" to 30, "RTP" to 10, "WIRE" to 5, "BRANCH" to 10),
        coreTransactionId = "CORE-" + digits(12),
    )

    fun settled(): SettledPayment {
        val rail = pick(Rail.ACH to 60, Rail.WIRE to 25, Rail.RTP to 15)
        return SettledPayment(
            settlementId = "STL-" + digits(10),
            paymentId = "PMT-" + digits(10),
            rail = rail,
            clearingSystem = when (rail) {
                Rail.ACH -> pick("FedACH" to 70, "EPN" to 30)
                Rail.WIRE -> "Fedwire Funds"
                Rail.RTP -> "RTP Network"
            },
            settlementDate = LocalDate.now(ZoneOffset.UTC),
            amount = money(10.0, 50_000.0),
            // ~2% carry an unexpected fee, which the reconciliation step flags.
            fee = if (Random.nextInt(100) < 2) BigDecimal("7.50") else pick(BigDecimal("0.00") to 50, BigDecimal("0.10") to 30, BigDecimal("0.50") to 20),
            currency = "USD",
            batchId = "BATCH-" + today() + "-" + digits(4),
        )
    }

    private fun amount(rail: Rail): BigDecimal = when (rail) {
        Rail.WIRE -> money(1_000.0, 250_000.0)
        Rail.ACH -> money(10.0, 15_000.0)
        Rail.RTP -> money(1.0, 5_000.0)
    }

    private fun money(min: Double, max: Double) = BigDecimal(Random.nextDouble(min, max)).setScale(2, RoundingMode.HALF_UP)
    private fun digits(n: Int) = (1..n).joinToString("") { Random.nextInt(10).toString() }
    private fun letters(n: Int) = (1..n).joinToString("") { ('A' + Random.nextInt(26)).toString() }
    private fun today() = LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.BASIC_ISO_DATE)

    private fun <T> pick(vararg weighted: Pair<T, Int>): T {
        var r = Random.nextInt(weighted.sumOf { it.second })
        for ((value, w) in weighted) {
            if (r < w) return value
            r -= w
        }
        return weighted.last().first
    }

    companion object {
        val COMPANIES = listOf(
            "Acme Industrial Supply", "Harbor View Dental LLC", "Pinecrest Logistics", "Sunrise Bakery Co",
            "Granite Peak Construction", "Bluebird Software Inc", "Lakeside Property Mgmt", "Evergreen Health Partners",
            "Copperline Electric", "Maple & Main Grocers", "Atlas Freight Brokers", "Riverbend Veterinary Clinic",
        )
        val BICS = listOf("CHASUS33", "BOFAUS3N", "CITIUS33", "WFBIUS6S", "DEUTDEFF", "HSBCGB2L", "BNPAFRPP", "MHCBJPJT")
        val ROUTING = listOf("021000021", "026009593", "121000248", "111000025", "071000013", "061000104")
        val RETURN_CODES = listOf(
            "R01" to "Insufficient funds", "R02" to "Account closed", "R03" to "No account / unable to locate account",
            "R04" to "Invalid account number", "R07" to "Authorization revoked by customer", "R08" to "Payment stopped",
            "R09" to "Uncollected funds", "R10" to "Customer advises not authorized", "R16" to "Account frozen",
        )
        val PAYEES = listOf(
            "Northstar Mortgage" to "Mortgage payment", "City Water & Power" to "Utility bill",
            "Brightline Wireless" to "Phone bill", "Oakridge Apartments" to "Rent", "Summit Auto Finance" to "Car loan",
            "State University Bursar" to "Tuition installment", "Guardian Mutual Insurance" to "Home insurance premium",
        )
    }
}
