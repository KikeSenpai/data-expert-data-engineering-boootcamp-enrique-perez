package com.aprendeingenieriadedatos.bootcampflink.geodataaggregation

import com.aprendeingenieriadedatos.bootcampflink.configs.KafkaConfig

object AggregatedGeoDataSource {
    const val TABLE_NAME = "processed_events"
    const val TIMESTAMP_PATTERN = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"

    val query =
        """
        CREATE TABLE $TABLE_NAME (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '$TIMESTAMP_PATTERN'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '${KafkaConfig.KAFKA_BOOTSTRAP_SERVERS}',
            'topic' = '${KafkaConfig.KAFKA_TOPIC}',
            'properties.group.id' = '${KafkaConfig.KAFKA_GROUP}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="${KafkaConfig.KAFKA_KEY}" password="${KafkaConfig.KAFKA_SECRET}";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
        """.trimIndent()
}
