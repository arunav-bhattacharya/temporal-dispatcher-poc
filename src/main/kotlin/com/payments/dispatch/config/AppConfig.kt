package com.payments.dispatch.config

import io.smallrye.config.ConfigMapping
import io.smallrye.config.WithDefault
import java.time.Duration

/** Which dispatcher the PoC runs. See the comparison page in docs/. */
enum class DispatchMode {
    /** Option 1: Temporal Schedules, plain claim-and-start loop. */
    POLLING,

    /** Option 2: Temporal Schedules + NOTIFY doorbell + Kotlin Flow dispatch pipeline. */
    REACTIVE;

    companion object {
        fun parse(value: String): DispatchMode = valueOf(value.trim().uppercase())
    }
}

fun AppConfig.dispatchMode(): DispatchMode = DispatchMode.parse(mode())

@ConfigMapping(prefix = "dispatch")
interface AppConfig {
    fun mode(): String
    fun taskQueue(): String
    fun temporal(): Temporal
    fun schedules(): Schedules
    fun workers(): Workers
    fun events(): Map<String, EventSettings>
    fun doorbell(): Doorbell
    fun reaper(): Reaper
    fun simulation(): Simulation

    interface Temporal {
        fun target(): String
        fun namespace(): String
    }

    interface Schedules {
        @WithDefault("true")
        fun autoSync(): Boolean
    }

    interface Workers {
        @WithDefault("true")
        fun enabled(): Boolean
        fun maxConcurrentActivities(): Int
        fun maxConcurrentWorkflowTasks(): Int
    }

    interface EventSettings {
        fun interval(): Duration
        fun doorbell(): Boolean
        fun priority(): Int
    }

    interface Doorbell {
        fun channel(): String
        fun minGap(): Duration
        fun pauseCache(): Duration
        fun host(): String
        fun port(): Int
        fun database(): String
        fun user(): String
        fun password(): String
    }

    interface Reaper {
        fun staleAfter(): Duration
        fun batch(): Int
    }

    interface Simulation {
        fun activityDelay(): Duration
        fun screeningHitRate(): Double
        fun scheduledLead(): Duration
    }
}
