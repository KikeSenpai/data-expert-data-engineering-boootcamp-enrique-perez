package com.aprendeingenieriadedatos.bootcampflink.geodataaggregation

object AggregatedGeoDataHostOperator {
    fun buildQuery(sourceTable: String): String =
        """
        SELECT
            window_start AS event_hour,
            host,
            COUNT(*) AS num_hits
        FROM TABLE(
            TUMBLE(
                DATA => TABLE $sourceTable,
                TIMECOL => DESCRIPTOR(window_timestamp),
                SIZE => INTERVAL '5' MINUTES))
        GROUP BY
            window_start,
            host;
        """.trimIndent()
}

object AggregatedGeoDataHostReferrerOperator {
    fun buildQuery(sourceTable: String): String =
        """
        SELECT
            window_start AS event_hour,
            host,
            referrer,
            COUNT(*) AS num_hits
        FROM TABLE(
            TUMBLE(
                DATA => TABLE $sourceTable,
                TIMECOL => DESCRIPTOR(window_timestamp),
                SIZE => INTERVAL '5' MINUTES))
        GROUP BY
            window_start,
            host,
            referrer;
        """.trimIndent()
}
