plugins {
    kotlin("jvm") version "2.4.10"
    kotlin("plugin.allopen") version "2.4.10"
    id("io.quarkus")
}

repositories {
    mavenCentral()
}

val quarkusPlatformVersion: String by project
val temporalSdkVersion: String by project
val exposedVersion: String by project
val kotlinLoggingVersion: String by project

dependencies {
    implementation(enforcedPlatform("io.quarkus.platform:quarkus-bom:$quarkusPlatformVersion"))

    // Quarkus
    implementation("io.quarkus:quarkus-kotlin")
    implementation("io.quarkus:quarkus-arc")
    implementation("io.quarkus:quarkus-config-yaml")
    implementation("io.quarkus:quarkus-rest-jackson")
    implementation("io.quarkus:quarkus-smallrye-health")

    // Postgres: JDBC pool for Exposed, Flyway for schema, Vert.x client for LISTEN/NOTIFY (option 2)
    implementation("io.quarkus:quarkus-jdbc-postgresql")
    implementation("io.quarkus:quarkus-agroal")
    implementation("io.quarkus:quarkus-flyway")
    implementation("org.flywaydb:flyway-database-postgresql")
    implementation("io.vertx:vertx-pg-client")
    implementation("com.ongres.scram:scram-client") // SCRAM-SHA-256 auth for the Vert.x LISTEN connection

    // Exposed
    implementation("org.jetbrains.exposed:exposed-core:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-jdbc:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-java-time:$exposedVersion")

    // Temporal
    implementation("io.temporal:temporal-sdk:$temporalSdkVersion")

    // Kotlin
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    implementation("io.github.oshai:kotlin-logging-jvm:$kotlinLoggingVersion")

    testImplementation("io.quarkus:quarkus-junit5")
    testImplementation("io.temporal:temporal-testing:$temporalSdkVersion")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test")
}

allOpen {
    annotation("jakarta.ws.rs.Path")
    annotation("jakarta.enterprise.context.ApplicationScoped")
    annotation("io.quarkus.test.junit.QuarkusTest")
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

kotlin {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
        javaParameters.set(true)
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}

tasks.withType<Test> {
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
    useJUnitPlatform()
}
