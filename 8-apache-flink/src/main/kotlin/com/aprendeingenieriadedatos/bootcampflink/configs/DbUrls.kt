package com.aprendeingenieriadedatos.bootcampflink.configs

object DbUrls {
    private fun postgresPort(): Int = DbConfig.POSTGRES_PORT ?: 5432

    val POSTGRES_URL: String by lazy {
        "jdbc:postgresql://${DbConfig.POSTGRES_HOST}:${postgresPort()}/${DbConfig.POSTGRES_DB}"
    }
}
