package com.aprendeingenieriadedatos.bootcampflink.pipelines

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

object PipelineRunner {
    fun run(
        tableEnv: StreamTableEnvironment,
        jobs: List<TablePipeline>,
    ) {
        // Register all jobs first (UDFs and DDLs)
        jobs.forEach { it.register(tableEnv) }

        // Create one StatementSet to execute all inserts in a single streaming job
        val statementSet = tableEnv.createStatementSet()
        jobs.forEach { it.addInserts(statementSet, tableEnv) }

        // Execute and block to keep the streaming job alive
        statementSet.execute().await()
    }
}