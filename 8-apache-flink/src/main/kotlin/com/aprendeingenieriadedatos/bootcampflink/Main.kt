package com.aprendeingenieriadedatos.bootcampflink

import com.aprendeingenieriadedatos.bootcampflink.geodataaggregation.AggregatedGeoDataHostOperator
import com.aprendeingenieriadedatos.bootcampflink.geodataaggregation.AggregatedGeoDataHostReferrerOperator
import com.aprendeingenieriadedatos.bootcampflink.geodataaggregation.AggregatedGeoDataHostReferrerSink
import com.aprendeingenieriadedatos.bootcampflink.geodataaggregation.AggregatedGeoDataHostSink
import com.aprendeingenieriadedatos.bootcampflink.geodataaggregation.AggregatedGeoDataSource
import com.aprendeingenieriadedatos.bootcampflink.geodataenrichment.EnrichedGeoDataOperator
import com.aprendeingenieriadedatos.bootcampflink.geodataenrichment.EnrichedGeoDataSink
import com.aprendeingenieriadedatos.bootcampflink.geodataenrichment.EnrichedGeoDataSource
import com.aprendeingenieriadedatos.bootcampflink.udfs.GetLocation
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

    tableEnv.createTemporarySystemFunction("GetLocation", GetLocation::class.java)

    // Enrich the data with geolocation information
    tableEnv.executeSql(EnrichedGeoDataSource.query)
    tableEnv.executeSql(EnrichedGeoDataSink.query)
    val enrichedGeoDataResult = tableEnv.sqlQuery(EnrichedGeoDataOperator.buildQuery(EnrichedGeoDataSource.TABLE_NAME))
    enrichedGeoDataResult.executeInsert(EnrichedGeoDataSink.TABLE_NAME).await()

    // Aggregate the data by hour
    tableEnv.executeSql(AggregatedGeoDataSource.query)
    tableEnv.executeSql(AggregatedGeoDataHostSink.query)
    tableEnv.executeSql(AggregatedGeoDataHostReferrerSink.query)
    val aggGeoDataHostResult = tableEnv.sqlQuery(AggregatedGeoDataHostOperator.buildQuery(EnrichedGeoDataSource.TABLE_NAME))
    val aggGeoDataHostReferrerResult = tableEnv.sqlQuery(AggregatedGeoDataHostReferrerOperator.buildQuery(EnrichedGeoDataSource.TABLE_NAME))
    aggGeoDataHostResult.executeInsert(AggregatedGeoDataHostSink.TABLE_NAME).await()
    aggGeoDataHostReferrerResult.executeInsert(AggregatedGeoDataHostReferrerSink.TABLE_NAME).await()
}
