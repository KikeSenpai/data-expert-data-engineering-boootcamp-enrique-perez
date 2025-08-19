package com.aprendeingenieriadedatos.bootcampflink.geodataaggregation

import com.aprendeingenieriadedatos.bootcampflink.configs.DbConfig
import com.aprendeingenieriadedatos.bootcampflink.configs.DbUrls

object AggregatedGeoDataHostSink {
    const val TABLE_NAME = "processed_events_aggregated_host"
    private val POSTGRES_URL = DbUrls.POSTGRES_URL

    val query =
        """
        CREATE TABLE $TABLE_NAME (
            event_hour TIMESTAMP(3),
            host VARCHAR,
            num_hits BIGINT,
            PRIMARY KEY (event_hour, host) NOT ENFORCED
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
    private val POSTGRES_URL = DbUrls.POSTGRES_URL

    val query =
        """
        CREATE TABLE $TABLE_NAME (
            event_hour TIMESTAMP(3),
            host VARCHAR,
            referrer VARCHAR,
            num_hits BIGINT,
            PRIMARY KEY (event_hour, host, referrer) NOT ENFORCED
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
