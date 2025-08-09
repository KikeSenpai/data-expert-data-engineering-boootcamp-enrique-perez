package com.aprendeingenieriadedatos.bootcampflink.rawgeodata

import com.aprendeingenieriadedatos.bootcampflink.configs.KafkaConfig

object EnrichedGeoDataSource {
    const val TABLE_NAME = "events"
    const val TIMESTAMP_PATTERN = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"

    val query = """
        CREATE TABLE $TABLE_NAME (
            url VARCHAR,
            referrer VARCHAR,
            user_agent VARCHAR,
            host VARCHAR,
            ip VARCHAR,
            headers VARCHAR,
            event_time VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '$TIMESTAMP_PATTERN')
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '${KafkaConfig.KAFKA_BOOTSTRAP_SERVERS}',
            'topic' = '${KafkaConfig.KAFKA_TOPIC}',
            'properties.group.id' = '${KafkaConfig.KAFKA_GROUP}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KafkaConfig.KAFKA_KEY}\" password=\"${KafkaConfig.KAFKA_SECRET}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """.trimIndent()
}