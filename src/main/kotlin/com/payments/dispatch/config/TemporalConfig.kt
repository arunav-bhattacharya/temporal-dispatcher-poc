package com.payments.dispatch.config

import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowClientOptions
import io.temporal.client.schedules.ScheduleClient
import io.temporal.client.schedules.ScheduleClientOptions
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.serviceclient.WorkflowServiceStubsOptions
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Disposes
import jakarta.enterprise.inject.Produces
import jakarta.inject.Singleton

private val log = KotlinLogging.logger {}

/** CDI producers for the Temporal client side. Stubs connect lazily on first call. */
@ApplicationScoped
class TemporalConfig(private val config: AppConfig) {

    @Produces
    @Singleton
    fun serviceStubs(): WorkflowServiceStubs {
        log.info { "Temporal target=${config.temporal().target()} namespace=${config.temporal().namespace()}" }
        return WorkflowServiceStubs.newServiceStubs(
            WorkflowServiceStubsOptions.newBuilder().setTarget(config.temporal().target()).build(),
        )
    }

    fun closeStubs(@Disposes stubs: WorkflowServiceStubs) {
        stubs.shutdown()
    }

    @Produces
    @Singleton
    fun workflowClient(stubs: WorkflowServiceStubs): WorkflowClient =
        WorkflowClient.newInstance(
            stubs,
            WorkflowClientOptions.newBuilder()
                .setNamespace(config.temporal().namespace())
                .setDataConverter(Json.temporalDataConverter)
                .build(),
        )

    @Produces
    @Singleton
    fun scheduleClient(stubs: WorkflowServiceStubs): ScheduleClient =
        ScheduleClient.newInstance(
            stubs,
            ScheduleClientOptions.newBuilder()
                .setNamespace(config.temporal().namespace())
                .setDataConverter(Json.temporalDataConverter)
                .build(),
        )
}
