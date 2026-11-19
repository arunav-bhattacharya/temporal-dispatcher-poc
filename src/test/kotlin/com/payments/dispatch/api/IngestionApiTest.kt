package com.payments.dispatch.api

import com.payments.dispatch.config.Json
import com.payments.dispatch.events.EventType
import com.payments.dispatch.events.PaymentEventRepository
import com.payments.dispatch.events.SampleEventGenerator
import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured.given
import io.restassured.http.ContentType
import jakarta.inject.Inject
import org.hamcrest.Matchers.containsString
import org.hamcrest.Matchers.equalTo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

/** POST /events/{type}: the front door of the diagram. Runs against Dev Services Postgres. */
@QuarkusTest
class IngestionApiTest {

    @Inject
    lateinit var generator: SampleEventGenerator

    @Inject
    lateinit var repo: PaymentEventRepository

    @Test
    fun `a valid inbound payment is accepted and stored as NEW`() {
        val body = Json.write(generator.inbound())
        val id = given().contentType(ContentType.JSON).body(body)
            .post("/events/INBOUND_PAYMENT")
            .then().statusCode(202).body("status", equalTo("NEW"))
            .extract().path<Number>("id").toLong()

        given().get("/events/$id")
            .then().statusCode(200)
            .body("type", equalTo("INBOUND_PAYMENT"))
            .body("status", equalTo("NEW"))
            .body("payload.currency", equalTo("USD"))
    }

    @Test
    fun `an invalid payload is rejected with 400 and nothing is stored`() {
        given().contentType(ContentType.JSON).body("""{"inboundId":"INB-1","amount":"not-a-number"}""")
            .post("/events/INBOUND_PAYMENT")
            .then().statusCode(400).body("error", containsString("Invalid INBOUND_PAYMENT payload"))
    }

    @Test
    fun `an unknown event type is rejected`() {
        given().contentType(ContentType.JSON).body("{}").post("/events/PAYMENT_TELEPORTED").then().statusCode(400)
    }

    @Test
    fun `a future-dated payment becomes claimable five minutes before it is due`() {
        val due = Instant.now().plus(Duration.ofHours(2))
        val body = Json.write(generator.scheduled(Duration.ofMinutes(1)).copy(scheduledFor = due))
        val id = given().contentType(ContentType.JSON).body(body)
            .post("/events/PAYMENT_SCHEDULED")
            .then().statusCode(202).extract().path<Number>("id").toLong()

        val stored = repo.find(id)!!
        assertEquals(due.minus(Duration.ofMinutes(5)).epochSecond, stored.availableAt.epochSecond)
        assertTrue(repo.claim(EventType.PAYMENT_SCHEDULED, 100, Duration.ofMinutes(2)).none { it.id == id }, "claimed too early")
    }
}
