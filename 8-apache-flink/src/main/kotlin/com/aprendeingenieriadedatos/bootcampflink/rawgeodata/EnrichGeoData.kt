package com.aprendeingenieriadedatos.bootcampflink.rawgeodata

object EnrichedGeoDataOperator {
    fun buildQuery(sourceTable: String, sinkTable: String): String {
        return """
            INSERT INTO $sinkTable
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
}