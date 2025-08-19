package com.aprendeingenieriadedatos.bootcampflink.geodataenrichment

object EnrichedGeoDataOperator {
    fun buildQuery(sourceTable: String): String =
        """
        SELECT
            ip,
            event_timestamp,
            referrer,
            host,
            url,
            GetLocation(ip) AS geodata
        FROM $sourceTable
        """.trimIndent()
}
