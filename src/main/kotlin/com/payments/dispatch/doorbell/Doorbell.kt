package com.payments.dispatch.doorbell

import com.payments.dispatch.config.AppConfig
import com.payments.dispatch.config.DispatchMode
import com.payments.dispatch.config.dispatchMode
import com.payments.dispatch.events.EventType
import io.github.oshai.kotlinlogging.KotlinLogging
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import io.temporal.api.enums.v1.ScheduleOverlapPolicy
import io.temporal.client.schedules.ScheduleClient
import jakarta.annotation.Priority
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.conflate
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.retryWhen
import kotlinx.coroutines.flow.shareIn
import org.postgresql.PGConnection
import java.sql.DriverManager
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min

private val log = KotlinLogging.logger {}

/**
 * Postgres LISTEN as a cold Kotlin Flow of notification payloads.
 * A dedicated JDBC connection (never a pooled one) waits up to a second at a time for news,
 * so cancellation is noticed within a second. Each collector opens its own connection.
 */
fun pgNotifications(jdbcUrl: String, user: String, password: String, channel: String): Flow<String> = flow {
    DriverManager.getConnection(jdbcUrl, user, password).use { conn ->
        conn.createStatement().use { it.execute("LISTEN $channel") }
        log.info { "Doorbell listening on '$channel'" }
        val pg = conn.unwrap(PGConnection::class.java)
        while (true) {
            currentCoroutineContext().ensureActive()
            pg.getNotifications(1_000)?.forEach { emit(it.parameter) }
        }
    }
}.flowOn(Dispatchers.IO)

/**
 * Option 2 only. Turns Postgres NOTIFY into Temporal Schedule triggers, built entirely with Kotlin Flow:
 *
 *   pgNotifications ─retryWhen─▶ mapNotNull(EventType) ─shareIn─▶ per type: filter ─▶ conflate ─▶ trigger, wait 2s
 *
 * NOTIFY has no backpressure, so each type conflates: while a trigger is in progress (or in its 2s gap),
 * extra rings collapse into one. A ring only means "look at the table", so dropping extras loses
 * nothing, and if the listener is down the schedule's interval ticks still drain the table.
 */
@ApplicationScoped
class Doorbell(
    private val config: AppConfig,
    private val schedules: ScheduleClient,
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val pausedCache = ConcurrentHashMap<EventType, Pair<Long, Boolean>>()

    val ringsReceived = AtomicLong()
    val triggersSent = AtomicLong()
    val skippedPaused = AtomicLong()

    /** Event types with the doorbell on (empty in polling mode). Set once at startup. */
    @Volatile
    var enabledTypes: Set<EventType> = emptySet()

    fun onStart(@Observes @Priority(30) event: StartupEvent) {
        if (config.dispatchMode() != DispatchMode.REACTIVE) return
        val db = config.doorbell()
        val types = config.events().filterValues { it.doorbell() }.keys.map(EventType::valueOf).toSet()
        val minGap = db.minGap().toMillis()
        val jdbcUrl = "jdbc:postgresql://${db.host()}:${db.port()}/${db.database()}"

        // One LISTEN connection, shared hot across all event types.
        val rings = pgNotifications(jdbcUrl, db.user(), db.password(), db.channel())
            .retryWhen { cause, attempt ->
                log.warn { "Doorbell connection lost (${cause.message}); interval ticks still dispatch. Reconnecting." }
                delay(min(1_000L * (attempt + 1), 30_000L))
                true
            }
            .onEach { ringsReceived.incrementAndGet() }
            .mapNotNull { payload -> EventType.entries.firstOrNull { it.name == payload } }
            .shareIn(scope, SharingStarted.Eagerly)

        types.forEach { type ->
            rings.filter { it == type }
                .conflate() // ten rings while we're busy still mean one trip to the door
                .onEach {
                    ring(type)
                    delay(minGap) // leading-edge throttle: the first ring triggers now
                }
                .launchIn(scope)
        }
        enabledTypes = types
    }

    private fun ring(type: EventType) {
        if (isPaused(type)) {
            skippedPaused.incrementAndGet()
            return
        }
        try {
            // BUFFER_ONE: if a dispatcher is already running, queue exactly one follow-up run.
            schedules.getHandle(type.scheduleId).trigger(ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE)
            triggersSent.incrementAndGet()
        } catch (e: Exception) {
            log.warn(e) { "Doorbell trigger failed for $type; the next interval tick will pick the rows up" }
        }
    }

    /** A manual trigger may not honor a pause, so check (cached) before ringing. */
    private fun isPaused(type: EventType): Boolean {
        val now = System.currentTimeMillis()
        val cached = pausedCache[type]
        if (cached != null && now - cached.first < config.doorbell().pauseCache().toMillis()) return cached.second
        val paused = runCatching { schedules.getHandle(type.scheduleId).describe().schedule.state?.isPaused ?: false }.getOrDefault(false)
        pausedCache[type] = now to paused
        return paused
    }

    /** Called by the admin API after a pause / resume so the doorbell reacts immediately. */
    fun forgetPausedState(type: EventType) {
        pausedCache.remove(type)
    }

    fun onStop(@Observes event: ShutdownEvent) {
        scope.cancel()
    }
}
