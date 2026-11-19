package com.payments.dispatch.dispatch

import com.payments.dispatch.config.AppConfig
import com.payments.dispatch.events.EventType
import io.github.oshai.kotlinlogging.KotlinLogging
import io.quarkus.runtime.StartupEvent
import io.temporal.api.enums.v1.ScheduleOverlapPolicy
import io.temporal.client.WorkflowOptions
import io.temporal.client.schedules.Schedule
import io.temporal.client.schedules.ScheduleActionStartWorkflow
import io.temporal.client.schedules.ScheduleAlreadyRunningException
import io.temporal.client.schedules.ScheduleClient
import io.temporal.client.schedules.ScheduleIntervalSpec
import io.temporal.client.schedules.ScheduleOptions
import io.temporal.client.schedules.SchedulePolicy
import io.temporal.client.schedules.ScheduleSpec
import io.temporal.client.schedules.ScheduleUpdate
import jakarta.annotation.Priority
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes

private val log = KotlinLogging.logger {}

/**
 * Creates or updates one Temporal Schedule per configured event type at startup.
 * An update keeps the schedule's current state, so a schedule paused during an incident stays paused.
 */
@ApplicationScoped
class ScheduleSyncer(private val config: AppConfig, private val schedules: ScheduleClient) {

    fun onStart(@Observes @Priority(20) event: StartupEvent) {
        if (!config.schedules().autoSync()) return
        config.events().forEach { (name, settings) ->
            val type = EventType.valueOf(name)
            val schedule = Schedule.newBuilder()
                .setAction(
                    ScheduleActionStartWorkflow.newBuilder()
                        .setWorkflowType(DispatcherWorkflow::class.java)
                        .setArguments(type.name)
                        .setOptions(
                            WorkflowOptions.newBuilder()
                                .setWorkflowId("dispatcher-${type.name}")
                                .setTaskQueue(config.taskQueue())
                                .build(),
                        )
                        .build(),
                )
                .setSpec(ScheduleSpec.newBuilder().setIntervals(listOf(ScheduleIntervalSpec(settings.interval()))).build())
                .setPolicy(
                    SchedulePolicy.newBuilder()
                        .setOverlap(ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP)
                        .setCatchupWindow(settings.interval())
                        .build(),
                )
                .build()
            try {
                schedules.createSchedule(type.scheduleId, schedule, ScheduleOptions.newBuilder().build())
                log.info { "Created schedule ${type.scheduleId} every ${settings.interval()}" }
            } catch (_: ScheduleAlreadyRunningException) {
                schedules.getHandle(type.scheduleId).update { input ->
                    ScheduleUpdate(Schedule.newBuilder(schedule).setState(input.description.schedule.state).build())
                }
                log.info { "Updated schedule ${type.scheduleId} every ${settings.interval()}" }
            }
        }
    }
}
