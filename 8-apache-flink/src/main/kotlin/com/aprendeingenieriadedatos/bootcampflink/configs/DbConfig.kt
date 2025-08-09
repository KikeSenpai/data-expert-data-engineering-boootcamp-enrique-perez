package com.aprendeingenieriadedatos.bootcampflink.configs

import io.github.cdimascio.dotenv.dotenv
import kotlin.text.isNotEmpty

object DbConfig {
    private val dotenv = dotenv {
        filename = ".env"
        ignoreIfMalformed = true
        ignoreIfMissing = true
    }

    val POSTGRES_DB = dotenv["POSTGRES_DB"] ?: "postgres"
    val POSTGRES_USER = dotenv["POSTGRES_USER"] ?: "postgres"
    val POSTGRES_PASSWORD = dotenv["POSTGRES_PASSWORD"] ?: "<PASSWORD>"
    val POSTGRES_PORT = dotenv["POSTGRES_PORT"]?.toIntOrNull() ?: 5432
    val POSTGRES_HOST = dotenv["POSTGRES_HOST"] ?: "localhost"

    init {
        require(POSTGRES_USER.isNotEmpty()) { "POSTGRES_USER environment variable is required" }
        require(POSTGRES_PASSWORD.isNotEmpty()) { "POSTGRES_PASSWORD environment variable is required" }
    }
}