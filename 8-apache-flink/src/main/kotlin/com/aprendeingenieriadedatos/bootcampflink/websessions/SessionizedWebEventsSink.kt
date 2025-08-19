package com.aprendeingenieriadedatos.bootcampflink.websessions

import com.aprendeingenieriadedatos.bootcampflink.configs.DbConfig
import com.aprendeingenieriadedatos.bootcampflink.configs.DbUrls

object SessionizedWebEventsSink {
    const val TABLE_NAME = "processed_events_sessions"
    private val POSTGRES_URL = DbUrls.POSTGRES_URL

    val query =
        """
        CREATE TABLE IF NOT EXISTS $TABLE_NAME (
            ip VARCHAR,
            host VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            events_in_session BIGINT,
            PRIMARY KEY (ip, host, session_start) NOT ENFORCED
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