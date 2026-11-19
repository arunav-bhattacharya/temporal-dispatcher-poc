package com.payments.dispatch.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.temporal.common.converter.DataConverter
import io.temporal.common.converter.DefaultDataConverter
import io.temporal.common.converter.JacksonJsonPayloadConverter

/**
 * One ObjectMapper for event payloads (jsonb) and Temporal payloads, so Kotlin data classes,
 * BigDecimal and java.time types round-trip the same way everywhere.
 */
object Json {
    val mapper: ObjectMapper = jacksonObjectMapper()
        .registerModule(JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

    /** Temporal's default converter chain with a Kotlin-aware Jackson converter. */
    val temporalDataConverter: DataConverter = DefaultDataConverter.newDefaultInstance()
        .withPayloadConverterOverrides(JacksonJsonPayloadConverter(mapper))

    inline fun <reified T> read(json: String): T = mapper.readValue(json, T::class.java)

    fun write(value: Any): String = mapper.writeValueAsString(value)
}
