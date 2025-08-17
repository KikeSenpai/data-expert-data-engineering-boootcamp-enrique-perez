package com.aprendeingenieriadedatos.bootcampflink.udfs

import org.apache.flink.table.functions.FunctionContext
import org.apache.flink.table.functions.ScalarFunction
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration

class GetLocation : ScalarFunction() {
    @Volatile private var baseUrl: String? = null
    @Volatile private var httpClient: HttpClient? = null
    @Volatile private var apiKey: String? = null

    override fun open(context: FunctionContext) {
        if (httpClient == null) {
            synchronized(this) {
                if (httpClient == null) {
                    httpClient =
                        HttpClient
                            .newBuilder()
                            .connectTimeout(Duration.ofSeconds(3))
                            .build()
                }
            }
        }

        if (baseUrl == null) {
            baseUrl = System.getenv("BASE_URL") ?: ""
        }
        if (apiKey == null) {
            apiKey = System.getenv("IP_CODING_KEY") ?: ""
        }
    }

    fun eval(ipAddress: String?): String? {
        if (ipAddress.isNullOrEmpty()) return null

        val url = baseUrl
        if (url.isNullOrEmpty()) return null

        // Build URI with query parameters: `?ip=<ip>&key=<apiKey>`
        val encodedIp = URLEncoder.encode(ipAddress.trim(), StandardCharsets.UTF_8)
        val encodedKey = URLEncoder.encode(apiKey, StandardCharsets.UTF_8)
        val uri = URI.create("$url?ip=${encodedIp.trim()}&key=$encodedKey")

        val request =
            HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(5))
                .header("Accept", "application/json")
                .GET()
                .build()

        val client = httpClient
        return try {
            val response = client?.send(request, HttpResponse.BodyHandlers.ofString())
            if (response?.statusCode() in 200..299) response?.body() else null
        } catch (_: Exception) {
            null
        }
    }
}
