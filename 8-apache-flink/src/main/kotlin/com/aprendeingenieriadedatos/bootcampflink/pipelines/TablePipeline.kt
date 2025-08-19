package com.aprendeingenieriadedatos.bootcampflink.pipelines

import org.apache.flink.table.api.StatementSet
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

/**
 * Minimal abstraction for a self-contained Table/SQL job unit.
 *
 * Implementations should:
 * - Register all required DDLs and UDFs in [register].
 * - Add their inserts into the provided [org.apache.flink.table.api.StatementSet] in [addInserts].
 */
interface TablePipeline {
    fun register(tableEnv: StreamTableEnvironment)

    fun addInserts(
        statementSet: StatementSet,
        tableEnv: StreamTableEnvironment,
    )
}