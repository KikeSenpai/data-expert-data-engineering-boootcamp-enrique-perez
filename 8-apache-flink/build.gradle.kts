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

configurations {
    register("flinkShadowJar") {
        exclude(group = "org.apache.flink", module = "force-shading")
        exclude(group = "com.google.code.findbugs", module = "jsr305")
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j-impl")
    }
}

val flinkVersion = "1.20.2"
val log4jVersion = "2.24.3"

dependencies {
    // Flink dependencies
    compileOnly("org.apache.flink:flink-streaming-java:${flinkVersion}")
    compileOnly("org.apache.flink:flink-clients:${flinkVersion}")
    compileOnly("org.apache.flink:flink-table-api-java:${flinkVersion}")
    compileOnly("org.apache.flink:flink-table-api-java-bridge:${flinkVersion}")
    compileOnly("org.apache.flink:flink-table-planner-loader:${flinkVersion}")
    compileOnly("org.apache.flink:flink-table-runtime:${flinkVersion}")

    // Logging dependencies
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:${log4jVersion}")
    runtimeOnly("org.apache.logging.log4j:log4j-api:${log4jVersion}")
    runtimeOnly("org.apache.logging.log4j:log4j-core:${log4jVersion}")
    
    // Other dependencies
    implementation("org.flywaydb:flyway-core:9.22.3")
    implementation("io.github.cdimascio:dotenv-kotlin:6.4.1")

    // HTTP4K dependencies
    implementation(platform("org.http4k:http4k-bom:6.12.0.0"))
    implementation("org.http4k:http4k-core")
    implementation("org.http4k:http4k-client-apache")
    implementation("org.http4k:http4k-format-jackson")
//    implementation("org.http4k:http4k-server-netty")

    // Test dependencies
    testImplementation(kotlin("test"))
}

// Make compileOnly dependencies available for tests:
sourceSets {
    main {
        compileClasspath += configurations["flinkShadowJar"]
        runtimeClasspath += configurations["flinkShadowJar"]
    }

    test {
        compileClasspath += configurations["flinkShadowJar"]
        runtimeClasspath += configurations["flinkShadowJar"]
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
        configurations = listOf(project.configurations.getByName("flinkShadowJar"))
        manifest {
            attributes("Main-Class" to mainClassName)
        }
        archiveClassifier.set("")
        isZip64 = true
        mergeServiceFiles()
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