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
            GetLocation(ip) AS geo_location
        FROM $sourceTable
        """.trimIndent()
}
