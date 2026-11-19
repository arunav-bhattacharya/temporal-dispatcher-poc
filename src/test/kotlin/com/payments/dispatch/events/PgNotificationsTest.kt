package com.payments.dispatch.events

import com.payments.dispatch.doorbell.pgNotifications
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/** The doorbell's Kotlin Flow hears the NOTIFY fired by the table trigger (real Postgres via Dev Services). */
@QuarkusTest
class PgNotificationsTest {

    @ConfigProperty(name = "quarkus.datasource.jdbc.url")
    lateinit var url: String

    @ConfigProperty(name = "quarkus.datasource.username")
    lateinit var user: String

    @ConfigProperty(name = "quarkus.datasource.password")
    lateinit var password: String

    @Inject
    lateinit var repo: PaymentEventRepository

    @Inject
    lateinit var generator: SampleEventGenerator

    @Test
    fun `an insert rings the doorbell with its event type`() = runBlocking {
        val ring = async { withTimeout(15_000) { pgNotifications(url, user, password, "payment_events").first() } }
        delay(1_500) // give LISTEN time to register before the insert
        repo.insertAll(generator.generate(EventType.PAYMENT_RETURNED, 3))
        assertEquals("PAYMENT_RETURNED", ring.await())
    }
}
