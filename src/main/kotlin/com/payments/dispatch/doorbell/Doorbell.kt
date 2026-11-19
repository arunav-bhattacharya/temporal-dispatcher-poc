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
import io.vertx.core.Vertx
import io.vertx.pgclient.PgConnectOptions
import io.vertx.pgclient.pubsub.PgSubscriber
import jakarta.annotation.Priority
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

private val log = KotlinLogging.logger {}

/**
 * Option 2 only. Turns Postgres NOTIFY into Temporal Schedule triggers.
 *
 *   INSERT ─trigger─▶ pg_notify('payment_events', type) ─▶ PgSubscriber ─▶ ring flow (conflated)
 *        ─▶ at most one trigger(BUFFER_ONE) per type every min-gap ─▶ DispatcherWorkflow
 *
 * NOTIFY has no backpressure, so each type's ring flow keeps only the latest ring
 * (buffer 1, DROP_OLDEST). A ring only means "look at the table": dropping extras loses nothing,
 * and if the listener is down the schedule's interval ticks still drain the table.
 */
@ApplicationScoped
class Doorbell(
    private val config: AppConfig,
    private val vertx: Vertx,
    private val schedules: ScheduleClient,
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val rings = ConcurrentHashMap<EventType, MutableSharedFlow<Unit>>()
    private val pausedCache = ConcurrentHashMap<EventType, Pair<Long, Boolean>>()
    private var subscriber: PgSubscriber? = null

    val ringsReceived = AtomicLong()
    val triggersSent = AtomicLong()
    val skippedPaused = AtomicLong()

    val enabledTypes: Set<EventType> get() = rings.keys

    fun onStart(@Observes @Priority(30) event: StartupEvent) {
        if (config.dispatchMode() != DispatchMode.REACTIVE) return
        val types = config.events().filterValues { it.doorbell() }.keys.map(EventType::valueOf)
        val minGap = config.doorbell().minGap().toMillis()

        types.forEach { type ->
            val flow = MutableSharedFlow<Unit>(extraBufferCapacity = 1, onBufferOverflow = BufferOverflow.DROP_OLDEST)
            rings[type] = flow
            scope.launch {
                flow.collect {
                    ring(type)
                    delay(minGap) // leading-edge throttle: first ring triggers now, the rest fold into one
                }
            }
        }

        val db = config.doorbell()
        val options = PgConnectOptions()
            .setHost(db.host()).setPort(db.port()).setDatabase(db.database())
            .setUser(db.user()).setPassword(db.password())
        val sub = PgSubscriber.subscriber(vertx, options)
            .reconnectPolicy { retries -> if (retries < 10_000) 1_000L else -1L }
        sub.channel(db.channel()).handler { payload ->
            ringsReceived.incrementAndGet()
            runCatching { EventType.valueOf(payload) }.getOrNull()?.let { rings[it]?.tryEmit(Unit) }
        }
        sub.connect()
            .onSuccess { log.info { "Doorbell listening on '${db.channel()}' for $types" } }
            .onFailure { log.warn(it) { "Doorbell could not connect; interval ticks still dispatch" } }
        subscriber = sub
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
        subscriber?.close()
        scope.cancel()
    }
}
