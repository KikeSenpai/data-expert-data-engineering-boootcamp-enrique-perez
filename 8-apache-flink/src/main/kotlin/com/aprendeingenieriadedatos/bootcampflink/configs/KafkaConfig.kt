package com.aprendeingenieriadedatos.bootcampflink.configs

import io.github.cdimascio.dotenv.dotenv

object KafkaConfig {
    private val dotenv = dotenv {
        filename = ".env"
        ignoreIfMalformed = true
        ignoreIfMissing = true
    }

    val KAFKA_BOOTSTRAP_SERVERS = dotenv["KAFKA_BOOTSTRAP_SERVERS"] ?: "localhost:9092"
    val KAFKA_TOPIC = dotenv["KAFKA_TOPIC"] ?: "default-topic"
    val KAFKA_GROUP = dotenv["KAFKA_GROUP"] ?: "default-group"
    val KAFKA_KEY = dotenv["KAFKA_KEY"] ?: ""
    val KAFKA_SECRET = dotenv["KAFKA_SECRET"] ?: ""

    init {
        require(KAFKA_KEY.isNotEmpty()) { "KAFKA_KEY environment variable is required" }
        require(KAFKA_SECRET.isNotEmpty()) { "KAFKA_SECRET environment variable is required" }
    }
}