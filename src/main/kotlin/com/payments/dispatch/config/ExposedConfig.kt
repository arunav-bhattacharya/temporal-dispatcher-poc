package com.payments.dispatch.config

import io.agroal.api.AgroalDataSource
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import jakarta.inject.Singleton
import org.jetbrains.exposed.v1.jdbc.Database

/** Exposed runs on the Quarkus-managed Agroal pool (Flyway owns the schema). */
@ApplicationScoped
class ExposedConfig {

    @Produces
    @Singleton
    fun database(dataSource: AgroalDataSource): Database = Database.connect(dataSource)
}
