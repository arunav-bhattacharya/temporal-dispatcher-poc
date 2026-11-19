package com.payments.dispatch.api

import com.fasterxml.jackson.databind.JsonNode
import com.payments.dispatch.config.AppConfig
import com.payments.dispatch.config.Json
import com.payments.dispatch.events.EventType
import com.payments.dispatch.events.NewEvent
import com.payments.dispatch.events.PaymentEventRepository
import com.payments.dispatch.events.SampleEventGenerator
import com.payments.dispatch.events.ScheduledPayment
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.BadRequestException
import jakarta.ws.rs.DefaultValue
import jakarta.ws.rs.GET
import jakarta.ws.rs.NotFoundException
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import jakarta.ws.rs.QueryParam
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import java.net.URI
import java.time.Duration
import java.time.Instant

/** Ingestion API, plus a load generator and lookup for the PoC. */
@Path("/events")
@Produces(MediaType.APPLICATION_JSON)
class EventsResource(
    private val generator: SampleEventGenerator,
    private val repo: PaymentEventRepository,
    private val config: AppConfig,
) {
    /**
     * Ingestion: a channel or partner posts one payment event. The body is validated against the
     * event type's payload class and stored as NEW; a dispatcher picks it up from there.
     * Future-dated payments become claimable shortly before they are due.
     */
    @POST
    @Path("/{type}")
    @Consumes(MediaType.APPLICATION_JSON)
    fun ingest(@PathParam("type") type: String, body: JsonNode?): Response {
        val eventType = parseType(type)
        if (body == null || !body.isObject) throw BadRequestException("Body must be a JSON object")
        val payload = try {
            Json.mapper.treeToValue(body, eventType.payloadClass)
        } catch (e: Exception) {
            val detail = e.message?.lineSequence()?.firstOrNull().orEmpty()
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(mapOf("error" to "Invalid ${eventType.name} payload", "detail" to detail)).build()
        }
        val now = Instant.now()
        val availableAt = if (payload is ScheduledPayment) {
            maxOf(now, payload.scheduledFor.minus(config.simulation().scheduledLead()))
        } else {
            now
        }
        val id = repo.insertOne(NewEvent(eventType, Json.write(payload), availableAt))
        return Response.accepted(
            mapOf("id" to id, "type" to eventType.name, "status" to "NEW", "availableAt" to availableAt.toString()),
        ).location(URI("/events/$id")).build()
    }

    /** Inserts [count] sample events of one type. Scheduled payments fall due within [dueWithinSeconds]. */
    @POST
    @Path("/generate")
    fun generate(
        @QueryParam("type") type: String?,
        @QueryParam("count") @DefaultValue("100") count: Int,
        @QueryParam("dueWithinSeconds") @DefaultValue("600") dueWithinSeconds: Long,
    ): Map<String, Any> {
        val eventType = parseType(type)
        require(count in 1..200_000) { "count must be between 1 and 200000" }
        val inserted = insertInChunks(eventType, count, Duration.ofSeconds(dueWithinSeconds))
        return mapOf("type" to eventType.name, "inserted" to inserted)
    }

    /** Inserts [count] events spread across all five types. */
    @POST
    @Path("/burst")
    fun burst(@QueryParam("count") @DefaultValue("1000") count: Int): Map<String, Int> {
        require(count in 5..500_000) { "count must be between 5 and 500000" }
        val per = count / EventType.entries.size
        return EventType.entries.associate { it.name to insertInChunks(it, per, Duration.ofMinutes(10)) }
    }

    @GET
    @Path("/{id}")
    fun get(@PathParam("id") id: Long): Map<String, Any?> {
        val e = repo.find(id) ?: throw NotFoundException("No event $id")
        return mapOf(
            "id" to e.id, "type" to e.type, "status" to e.status, "availableAt" to e.availableAt.toString(), "workflowId" to e.workflowId,
            "outcome" to e.outcome, "lastError" to e.lastError, "payload" to Json.mapper.readTree(e.payloadJson),
        )
    }

    /** Sample payload for one type, without inserting anything. */
    @GET
    @Path("/sample/{type}")
    fun sample(@PathParam("type") type: String): Any = Json.mapper.readTree(generator.generate(parseType(type), 1).single().payloadJson)

    private fun insertInChunks(type: EventType, count: Int, dueWithin: Duration): Int =
        (0 until count).chunked(1_000).sumOf { chunk -> repo.insertAll(generator.generate(type, chunk.size, dueWithin)) }

    private fun parseType(type: String?): EventType =
        type?.let { runCatching { EventType.valueOf(it.uppercase()) }.getOrNull() }
            ?: throw BadRequestException("type must be one of ${EventType.entries}")
}
