package com.aprendeingenieriadedatos.bootcampflink.websessions

import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.Session
import org.apache.flink.table.api.Expressions.*

object SessionizedWebEventsOperator {

    /**
     * Builds a Table representing 5-minute sessionized web events:
     * - Session gap of 5 minutes on 'window_timestamp'
     * - Grouped by ip and host
     * - Outputs session_start, session_end, and events_in_session
     *
     * Reference: Flink Table API session windows
     * https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/tableapi/#session-session-windows
     */
    fun buildTable(tableEnv: StreamTableEnvironment, sourceTable: String): Table {
        val input = tableEnv.from(sourceTable)

        return input
            .window(
                Session
                    .withGap(lit(5).minutes())
                    .on(`$`("window_timestamp"))
                    .`as`("w")
            )
            .groupBy(`$`("w"), `$`("ip"), `$`("host"))
            .select(
                `$`("ip"),
                `$`("host"),
                `$`("w").start().`as`("session_start"),
                `$`("w").end().`as`("session_end"),
                `$`("ip").count().`as`("events_in_session"),
            )
    }
}