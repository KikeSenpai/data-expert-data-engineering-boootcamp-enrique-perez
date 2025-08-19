package com.aprendeingenieriadedatos.bootcampflink.geodataenrichment

import com.aprendeingenieriadedatos.bootcampflink.configs.DbConfig
import com.aprendeingenieriadedatos.bootcampflink.configs.DbUrls

object EnrichedGeoDataSink {
    const val TABLE_NAME = "processed_events"
    private val POSTGRES_URL = DbUrls.POSTGRES_URL

    val query =
        """
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
