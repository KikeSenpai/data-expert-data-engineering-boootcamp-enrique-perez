package com.aprendeingenieriadedatos.bootcampflink

import com.aprendeingenieriadedatos.bootcampflink.pipelines.PipelineRunner
import com.aprendeingenieriadedatos.bootcampflink.pipelines.AggregationsPipeline
import com.aprendeingenieriadedatos.bootcampflink.pipelines.EnrichmentPipeline
import com.aprendeingenieriadedatos.bootcampflink.pipelines.SessionizationPipeline
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import java.time.Duration

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    env.parallelism = 2
    env.enableCheckpointing(Duration.ofSeconds(10).toMillis())
    with(env.checkpointConfig) {
        checkpointTimeout = Duration.ofMinutes(1).toMillis()
        minPauseBetweenCheckpoints = Duration.ofSeconds(10).toMillis()
        tolerableCheckpointFailureNumber = 2
        maxConcurrentCheckpoints = 1
        enableUnalignedCheckpoints()
    }

    val settings =
        EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    // Run all jobs together; each job registers its own UDFs/DDL and inserts.
    PipelineRunner.run(
        tableEnv,
        listOf(
            EnrichmentPipeline,
            AggregationsPipeline,
            SessionizationPipeline,
        ),
    )
}
