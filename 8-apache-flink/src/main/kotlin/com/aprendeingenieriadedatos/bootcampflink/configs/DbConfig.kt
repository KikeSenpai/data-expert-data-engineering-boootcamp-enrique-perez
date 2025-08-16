package com.aprendeingenieriadedatos.bootcampflink.configs

import kotlin.text.isNotEmpty

object DbConfig {
    val POSTGRES_DB = System.getenv("POSTGRES_DB") ?: "postgres"
    val POSTGRES_USER = System.getenv("POSTGRES_USER") ?: ""
    val POSTGRES_PASSWORD = System.getenv("POSTGRES_PASSWORD") ?: ""
    val POSTGRES_PORT = (System.getenv("POSTGRES_PORT") ?: "5432").toIntOrNull()
    val POSTGRES_HOST = System.getenv("POSTGRES_HOST") ?: ""

    init {
        require(POSTGRES_USER.isNotEmpty()) { "POSTGRES_USER environment variable is required" }
        require(POSTGRES_PASSWORD.isNotEmpty()) { "POSTGRES_PASSWORD environment variable is required" }
    }
}
