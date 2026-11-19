package com.payments.dispatch.api

import com.payments.dispatch.config.AppConfig
import com.payments.dispatch.config.dispatchMode
import com.payments.dispatch.dispatch.DispatchConfigRepository
import com.payments.dispatch.dispatch.DispatchLimits
import com.payments.dispatch.doorbell.Doorbell
import com.payments.dispatch.events.EventType
import com.payments.dispatch.events.PaymentEventRepository
import io.temporal.api.enums.v1.ScheduleOverlapPolicy
import io.temporal.client.schedules.ScheduleClient
import jakarta.ws.rs.BadRequestException
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.DefaultValue
import jakarta.ws.rs.GET
import jakarta.ws.rs.POST
import jakarta.ws.rs.PUT
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import jakarta.ws.rs.QueryParam
import jakarta.ws.rs.core.MediaType
import java.time.Duration

data class LimitsDto(
    val batchSize: Int,
    val maxInflight: Int,
    val startsPerSecond: Int,
    val parallelism: Int,
    val timeBudgetSeconds: Long,
    val leaseSeconds: Long,
    val enabled: Boolean,
) {
    fun toLimits() = DispatchLimits(batchSize, maxInflight, startsPerSecond, parallelism, Duration.ofSeconds(timeBudgetSeconds), Duration.ofSeconds(leaseSeconds), enabled)

    companion object {
        fun of(l: DispatchLimits) = LimitsDto(l.batchSize, l.maxInflight, l.startsPerSecond, l.parallelism, l.timeBudget.seconds, l.lease.seconds, l.enabled)
    }
}

/** Ops controls: the same levers a future load governor would use. */
@Path("/admin")
@Produces(MediaType.APPLICATION_JSON)
class AdminResource(
    private val config: AppConfig,
    private val repo: PaymentEventRepository,
    private val limits: DispatchConfigRepository,
    private val schedules: ScheduleClient,
    private val doorbell: Doorbell,
) {
    @GET
    @Path("/stats")
    fun stats(): Map<String, Any> = mapOf(
        "mode" to config.dispatchMode().name,
        "events" to repo.countsByTypeAndStatus(),
        "doorbell" to mapOf(
            "types" to doorbell.enabledTypes.map { it.name },
            "ringsReceived" to doorbell.ringsReceived.get(),
            "triggersSent" to doorbell.triggersSent.get(),
            "skippedPaused" to doorbell.skippedPaused.get(),
        ),
    )

    @GET
    @Path("/schedules")
    fun listSchedules(): List<Map<String, Any?>> = config.events().map { (name, settings) ->
        val type = EventType.valueOf(name)
        val d = runCatching { schedules.getHandle(type.scheduleId).describe() }.getOrNull()
        mapOf(
            "eventType" to name,
            "scheduleId" to type.scheduleId,
            "interval" to settings.interval().toString(),
            "priority" to settings.priority(),
            "doorbell" to settings.doorbell(),
            "paused" to d?.schedule?.state?.isPaused,
            "note" to d?.schedule?.state?.note,
            "nextRun" to d?.info?.nextActionTimes?.firstOrNull()?.toString(),
            "exists" to (d != null),
        )
    }

    @POST
    @Path("/schedules/{type}/pause")
    fun pause(@PathParam("type") type: String, @QueryParam("note") @DefaultValue("paused via admin API") note: String): Map<String, Any> {
        val t = parseType(type)
        schedules.getHandle(t.scheduleId).pause(note)
        doorbell.forgetPausedState(t)
        return mapOf("scheduleId" to t.scheduleId, "paused" to true, "note" to note)
    }

    @POST
    @Path("/schedules/{type}/resume")
    fun resume(@PathParam("type") type: String, @QueryParam("note") @DefaultValue("resumed via admin API") note: String): Map<String, Any> {
        val t = parseType(type)
        schedules.getHandle(t.scheduleId).unpause(note)
        doorbell.forgetPausedState(t)
        return mapOf("scheduleId" to t.scheduleId, "paused" to false, "note" to note)
    }

    /** Runs a dispatcher now (BUFFER_ONE: at most one queued behind a running one). */
    @POST
    @Path("/schedules/{type}/trigger")
    fun trigger(@PathParam("type") type: String): Map<String, Any> {
        val t = parseType(type)
        schedules.getHandle(t.scheduleId).trigger(ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE)
        return mapOf("scheduleId" to t.scheduleId, "triggered" to true)
    }

    @GET
    @Path("/limits")
    fun getLimits(): Map<String, LimitsDto> = limits.all().mapValues { LimitsDto.of(it.value) }

    /** Changes take effect on the next dispatcher run: no redeploy, no restart. */
    @PUT
    @Path("/limits/{type}")
    @Consumes(MediaType.APPLICATION_JSON)
    fun putLimits(@PathParam("type") type: String, body: LimitsDto): LimitsDto {
        val t = parseType(type)
        if (body.batchSize < 1 || body.maxInflight < 1 || body.startsPerSecond < 1 || body.parallelism < 1) {
            throw BadRequestException("batchSize, maxInflight, startsPerSecond and parallelism must be ≥ 1")
        }
        limits.update(t, body.toLimits())
        return LimitsDto.of(limits.load(t))
    }

    private fun parseType(type: String): EventType =
        runCatching { EventType.valueOf(type.uppercase()) }.getOrNull()
            ?: throw BadRequestException("type must be one of ${EventType.entries}")
}
