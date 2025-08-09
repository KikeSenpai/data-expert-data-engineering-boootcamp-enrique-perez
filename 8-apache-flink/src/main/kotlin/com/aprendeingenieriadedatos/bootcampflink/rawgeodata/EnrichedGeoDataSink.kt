package com.aprendeingenieriadedatos.bootcampflink.rawgeodata

import com.aprendeingenieriadedatos.bootcampflink.configs.DbConfig

object EnrichedGeoDataSink {
    const val TABLE_NAME = "processed_events"
    val POSTGRES_URL = "jdbc:postgresql://${DbConfig.POSTGRES_HOST}:${DbConfig.POSTGRES_PORT}/${DbConfig.POSTGRES_DB}"

    val query = """
        CREATE TABLE $TABLE_NAME (
            ip VARCHAR,
            event_timestamp TIMESTAMP(3),
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR
        ) WITH (
            'connector' = 'jdbc',
            'url' = '$POSTGRES_URL',
            'table-name' = '$TABLE_NAME',
            'username' = '${DbConfig.POSTGRES_USER}',
            'password' = '${DbConfig.POSTGRES_PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        );
    """.trimIndent()
}