@file:Suppress("UnstableApiUsage")

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    kotlin("jvm") version "2.2.0"
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
    id("org.jlleitschuh.gradle.ktlint") version "13.0.0"
    id("org.flywaydb.flyway") version "9.22.3"
}

kotlin {
    jvmToolchain(11)
}

group = "com.aprendeingenieriadedatos.bootcampflink"
version = "1.0-SNAPSHOT"

val mainClassName = "com.aprendeingenieriadedatos.bootcampflink.MainKt"

application {
    mainClass.set(mainClassName)
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://repository.apache.org/content/repositories/snapshots")
        mavenContent {
            snapshotsOnly()
        }
    }
}

ktlint {
    reporters {
        reporter(org.jlleitschuh.gradle.ktlint.reporter.ReporterType.CHECKSTYLE)
        reporter(org.jlleitschuh.gradle.ktlint.reporter.ReporterType.HTML)
    }
    filter {
        exclude("**/build/**")
        include("**/src/**/*.kt")
    }
}

val flinkShadowJar by configurations.registering {
    exclude(group = "org.apache.flink", module = "force-shading")
    exclude(group = "com.google.code.findbugs", module = "jsr305")
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
    exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j-impl")
}

val flinkVersion = "1.20.2"
val log4jVersion = "2.24.3"
val kafkaConnectorVersion = "3.4.0"
val jdbcConnectorVersion = "3.3.0"

dependencies {
    // Flink core dependencies
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-clients:$flinkVersion")
    compileOnly("org.apache.flink:flink-table-api-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-table-api-java-bridge:$flinkVersion")
    compileOnly("org.apache.flink:flink-table-planner-loader:$flinkVersion")
    compileOnly("org.apache.flink:flink-table-runtime:$flinkVersion")

    // Flink connectors and drivers
    flinkShadowJar("org.apache.flink:flink-connector-kafka:${kafkaConnectorVersion + "-" + flinkVersion.substringBeforeLast(".")}")
    flinkShadowJar("org.apache.flink:flink-connector-jdbc:${jdbcConnectorVersion + "-" + flinkVersion.substringBeforeLast(".")}")

    // Logging dependencies
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
    testRuntimeOnly("org.apache.logging.log4j:log4j-api:$log4jVersion")
    testRuntimeOnly("org.apache.logging.log4j:log4j-core:$log4jVersion")

    // http4K dependencies
    implementation(platform("org.http4k:http4k-bom:6.12.0.0"))
    implementation("org.http4k:http4k-core")
    implementation("org.http4k:http4k-client-apache")
    implementation("org.http4k:http4k-format-jackson")

    // Other dependencies
    implementation("org.flywaydb:flyway-core:9.22.3")
    runtimeOnly("org.postgresql:postgresql:42.7.7")

    // Test dependencies
    testImplementation(kotlin("test"))
}

// Make compileOnly dependencies available for tests:
sourceSets {
    main {
        compileClasspath += flinkShadowJar.get()
        runtimeClasspath += flinkShadowJar.get()
    }

    test {
        compileClasspath += flinkShadowJar.get()
        runtimeClasspath += flinkShadowJar.get()
    }
}

flyway {
    url = "jdbc:postgresql://localhost:5432/postgres"
    user = "postgres"
    password = "postgres"
    driver = "org.postgresql.Driver"
}

tasks {
    named<JavaExec>("run") {
        description = "Runs the application with the provided arguments"
        classpath = sourceSets.main.get().runtimeClasspath
    }

    named<ShadowJar>("shadowJar") {
        description = "Create the application uber jar with all dependencies"
        configurations = listOf(
            flinkShadowJar.get(),
            project.configurations.runtimeClasspath.get(),
        )
        manifest {
            attributes("Main-Class" to mainClassName)
        }
        archiveClassifier.set("")
        isZip64 = true
        mergeServiceFiles()

        exclude("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
        exclude("org.apache.logging.log4j:log4j-api:$log4jVersion")
        exclude("org.apache.logging.log4j:log4j-core:$log4jVersion")
        exclude("org.slf4j:slf4j-log4j12:.*")
    }

    register<Exec>("dockerUp") {
        group = "docker"
        description = "Spins up the local Flink environment."
        commandLine("docker", "compose", "up", "-d")
    }

    register<Exec>("dockerDown") {
        group = "docker"
        description = "Stops the local Flink environment."
        commandLine("docker", "compose", "down", "--volumes")
    }

    register<Exec>("submitFlinkJob") {
        group = "flink"
        description = "Submits job to local Flink cluster."
        commandLine(
            "docker",
            "compose",
            "exec",
            "flink-jobmanager",
            "./bin/flink",
            "run",
            "-d",
            "/opt/src/8-apache-flink-$version.jar",
        )
    }

    test {
        description = "Runs the unit tests"
        useJUnitPlatform()
    }
}
