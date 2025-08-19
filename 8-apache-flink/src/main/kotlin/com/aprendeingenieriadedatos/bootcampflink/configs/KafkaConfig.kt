package com.aprendeingenieriadedatos.bootcampflink.configs

object KafkaConfig {
    val KAFKA_BOOTSTRAP_SERVERS = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: ""
    val KAFKA_TOPIC = System.getenv("KAFKA_TOPIC") ?: ""
    val KAFKA_GROUP = System.getenv("KAFKA_GROUP") ?: ""
    val KAFKA_KEY = System.getenv("KAFKA_KEY") ?: ""
    val KAFKA_SECRET = System.getenv("KAFKA_SECRET") ?: ""

    init {
        require(KAFKA_KEY.isNotEmpty()) { "KAFKA_KEY environment variable is required" }
        require(KAFKA_SECRET.isNotEmpty()) { "KAFKA_SECRET environment variable is required" }
    }
}
