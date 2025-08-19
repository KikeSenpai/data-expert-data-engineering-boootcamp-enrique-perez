package com.aprendeingenieriadedatos.bootcampflink.pipelines

import com.aprendeingenieriadedatos.bootcampflink.geodataaggregation.AggregatedGeoDataSource
import com.aprendeingenieriadedatos.bootcampflink.websessions.SessionizedWebEventsOperator
import com.aprendeingenieriadedatos.bootcampflink.websessions.SessionizedWebEventsSink
import org.apache.flink.table.api.StatementSet
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

object SessionizationPipeline : TablePipeline {
    override fun register(tableEnv: StreamTableEnvironment) {
        tableEnv.executeSql(SessionizedWebEventsSink.query)
    }

    override fun addInserts(
        statementSet: StatementSet,
        tableEnv: StreamTableEnvironment,
    ) {
        val sessionsTable = SessionizedWebEventsOperator.buildTable(tableEnv, AggregatedGeoDataSource.TABLE_NAME)
        statementSet.addInsert(SessionizedWebEventsSink.TABLE_NAME, sessionsTable)
    }
}