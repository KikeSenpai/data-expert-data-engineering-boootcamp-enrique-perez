package com.aprendeingenieriadedatos.bootcampflink.pipelines

import com.aprendeingenieriadedatos.bootcampflink.geodataenrichment.EnrichedGeoDataOperator
import com.aprendeingenieriadedatos.bootcampflink.geodataenrichment.EnrichedGeoDataSink
import com.aprendeingenieriadedatos.bootcampflink.geodataenrichment.EnrichedGeoDataSource
import com.aprendeingenieriadedatos.bootcampflink.udfs.GetLocation
import org.apache.flink.table.api.StatementSet
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

object EnrichmentPipeline : TablePipeline {
    override fun register(tableEnv: StreamTableEnvironment) {
        tableEnv.createTemporarySystemFunction("GetLocation", GetLocation::class.java)
        tableEnv.executeSql(EnrichedGeoDataSource.query)
        tableEnv.executeSql(EnrichedGeoDataSink.query)
    }

    override fun addInserts(
        statementSet: StatementSet,
        tableEnv: StreamTableEnvironment,
    ) {
        val enrichedGeoDataTable =
            tableEnv.sqlQuery(
                EnrichedGeoDataOperator.buildQuery(EnrichedGeoDataSource.TABLE_NAME),
            )
        statementSet.addInsert(EnrichedGeoDataSink.TABLE_NAME, enrichedGeoDataTable)
    }
}
