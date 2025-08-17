package com.aprendeingenieriadedatos.bootcampflink.geodataaggregation

import com.aprendeingenieriadedatos.bootcampflink.configs.DbConfig

object AggregatedGeoDataHostSink {
    const val TABLE_NAME = "processed_events_aggregated_host"
    val POSTGRES_URL = "jdbc:postgresql://${DbConfig.POSTGRES_HOST}:${DbConfig.POSTGRES_PORT}/${DbConfig.POSTGRES_DB}"

    val query =
        """
        CREATE TABLE $TABLE_NAME (
            event_hour TIMESTAMP(3),
            host VARCHAR,
            num_of_hints BIGINT
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

object AggregatedGeoDataHostReferrerSink {
    const val TABLE_NAME = "processed_events_aggregated_host_referrer"
    val POSTGRES_URL = "jdbc:postgresql://${DbConfig.POSTGRES_HOST}:${DbConfig.POSTGRES_PORT}/${DbConfig.POSTGRES_DB}"

    val query =
        """
        CREATE TABLE $TABLE_NAME (
            event_hour TIMESTAMP(3),
            host VARCHAR,
            referrer VARCHAR,
            num_of_hints BIGINT
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
