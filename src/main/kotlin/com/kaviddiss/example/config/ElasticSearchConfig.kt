package com.kaviddiss.example.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.NodeBuilder
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate
import org.springframework.data.elasticsearch.core.EntityMapper
import org.springframework.data.elasticsearch.core.geo.CustomGeoModule
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories
import java.io.IOException

@Configuration
@EnableElasticsearchRepositories(basePackages = arrayOf("com.kaviddiss.example.repo"))
class ElasticSearchConfig {
    val log = LoggerFactory.getLogger(javaClass)

    class KotlinEntityMapper: EntityMapper {
        private val objectMapper = createObjectMapper()

        private fun createObjectMapper(): ObjectMapper {
            val mapper = jacksonObjectMapper()
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
            mapper.registerModule(CustomGeoModule())
            return mapper
        }

        @Throws(IOException::class)
        override fun mapToString(`object`: Any): String {
            return objectMapper.writeValueAsString(`object`)
        }

        @Throws(IOException::class)
        override fun <T> mapToObject(source: String, clazz: Class<T>): T {
            return objectMapper.readValue(source, clazz)
        }
    }

    @Bean
    fun kotlinEntityMapper() = KotlinEntityMapper()

    @Bean
    fun elasticsearchTemplate(@Value("\${elasticsearch.http.enabled}") httpEnabled: String,
                              @Value("\${elasticsearch.http.port}") httpPort: Int): ElasticsearchTemplate {
        val elasticsearchSettings = Settings.settingsBuilder()
                .put("http.enabled", httpEnabled)
                .put("http.port", httpPort)
                .put("path.data", "data")
                .put("path.home", System.getProperty("user.dir"))

        return ElasticsearchTemplate(NodeBuilder()
                .local(true)
                .settings(elasticsearchSettings.build())
                .node()
                .client(), kotlinEntityMapper())
    }
}