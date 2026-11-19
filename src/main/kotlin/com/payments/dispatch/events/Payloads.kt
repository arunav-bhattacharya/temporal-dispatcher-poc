package com.payments.dispatch.events

import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate

/*
 * Sample payloads for the five event types. Stored as jsonb in payment_events.payload
 * and loaded by each workflow's first activity.
 */

enum class Rail { ACH, WIRE, RTP }

enum class Direction { DEBIT, CREDIT }

/** A counterparty outside the bank. */
data class Party(
    val name: String,
    val accountNumber: String,
    val routingNumber: String? = null,
    val bankBic: String? = null,
)

/** Money arrived for one of our customers (wire, ACH credit or RTP). */
data class InboundPayment(
    val inboundId: String,
    val rail: Rail,
    val amount: BigDecimal,
    val currency: String,
    val originator: Party,
    val beneficiaryAccountId: String,
    val remittanceInfo: String,
    /** IMAD for wires, trace number for ACH, end-to-end ID for RTP. */
    val referenceNumber: String,
    val receivedAt: Instant,
)

/** An ACH payment we sent came back with a NACHA return code. */
data class ReturnedPayment(
    val paymentId: String,
    val originalTraceNumber: String,
    val returnCode: String,
    val returnReason: String,
    val amount: BigDecimal,
    val currency: String,
    val accountId: String,
    val rail: Rail,
    val returnedAt: Instant,
)

/** A customer scheduled a payment for a future date. */
data class ScheduledPayment(
    val paymentId: String,
    val debtorAccountId: String,
    val creditor: Party,
    val amount: BigDecimal,
    val currency: String,
    val rail: Rail,
    val scheduledFor: Instant,
    val frequency: String,
    val memo: String,
    val initiatedBy: String,
    val idempotencyKey: String,
)

/** The core banking system posted a payment to an account. */
data class PostedPayment(
    val paymentId: String,
    val accountId: String,
    val direction: Direction,
    val amount: BigDecimal,
    val currency: String,
    val postedAt: Instant,
    val valueDate: LocalDate,
    val channel: String,
    val coreTransactionId: String,
)

/** The clearing system confirmed settlement of a payment. */
data class SettledPayment(
    val settlementId: String,
    val paymentId: String,
    val rail: Rail,
    val clearingSystem: String,
    val settlementDate: LocalDate,
    val amount: BigDecimal,
    val fee: BigDecimal,
    val currency: String,
    val batchId: String,
)
