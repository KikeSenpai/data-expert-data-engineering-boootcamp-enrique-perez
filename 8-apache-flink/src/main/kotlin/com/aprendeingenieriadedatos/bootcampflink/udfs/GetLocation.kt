package com.aprendeingenieriadedatos.bootcampflink.udfs

import io.github.cdimascio.dotenv.dotenv
import org.apache.flink.table.functions.ScalarFunction
import org.http4k.client.ApacheClient
import org.http4k.core.Method.GET
import org.http4k.core.Request
import org.http4k.format.Jackson.asJsonObject
import org.http4k.format.Jackson.mapper

class GetLocation : ScalarFunction() {
    private val dotenv = dotenv {
        filename = ".env"
        ignoreIfMalformed = true
        ignoreIfMissing = true
    }
    private val url = "https://api.ip2location.io"
    private val apiKey = dotenv["IP_CODING_KEY"] ?: ""

    fun eval(ipAddress: String): String {
        val client = ApacheClient()

        val request =
            Request(GET, url)
                .query("ip", ipAddress)
                .query("key", apiKey)

        val response = client(request)
        if (response.status.code != 200) {
            return "{}"
        }

        val data = response.bodyString().asJsonObject()
        val country = data.at("/country_code").textValue() ?: ""
        val state = data.at("/region_name").textValue() ?: ""
        val city = data.at("/city_name").textValue() ?: ""

        return mapper.writeValueAsString(
            mapOf(
                "country" to country,
                "state" to state,
                "city" to city
            )
        )
    }
}