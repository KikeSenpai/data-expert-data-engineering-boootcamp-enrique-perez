package com.aprendeingenieriadedatos.bootcampflink.pipelines

import com.aprendeingenieriadedatos.bootcampflink.geodataaggregation.AggregatedGeoDataHostOperator
import com.aprendeingenieriadedatos.bootcampflink.geodataaggregation.AggregatedGeoDataHostReferrerOperator
import com.aprendeingenieriadedatos.bootcampflink.geodataaggregation.AggregatedGeoDataHostReferrerSink
import com.aprendeingenieriadedatos.bootcampflink.geodataaggregation.AggregatedGeoDataHostSink
import com.aprendeingenieriadedatos.bootcampflink.geodataaggregation.AggregatedGeoDataSource
import org.apache.flink.table.api.StatementSet
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

object AggregationsPipeline : TablePipeline {
    override fun register(tableEnv: StreamTableEnvironment) {
        // Shared source for all aggregations
        tableEnv.executeSql(AggregatedGeoDataSource.query)
        // Two sinks
        tableEnv.executeSql(AggregatedGeoDataHostSink.query)
        tableEnv.executeSql(AggregatedGeoDataHostReferrerSink.query)
    }

    override fun addInserts(
        statementSet: StatementSet,
        tableEnv: StreamTableEnvironment,
    ) {
        val aggGeoDataHostTable =
            tableEnv.sqlQuery(
                AggregatedGeoDataHostOperator.buildQuery(AggregatedGeoDataSource.TABLE_NAME),
            )
        val aggGeoDataHostReferrerTable =
            tableEnv.sqlQuery(
                AggregatedGeoDataHostReferrerOperator.buildQuery(AggregatedGeoDataSource.TABLE_NAME),
            )
        statementSet.addInsert(AggregatedGeoDataHostSink.TABLE_NAME, aggGeoDataHostTable)
        statementSet.addInsert(AggregatedGeoDataHostReferrerSink.TABLE_NAME, aggGeoDataHostReferrerTable)
    }
}
