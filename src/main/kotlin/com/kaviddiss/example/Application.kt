package com.kaviddiss.example

import com.kaviddiss.example.service.GeoNameLoader
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.web.support.SpringBootServletInitializer
import org.springframework.context.annotation.Bean

@SpringBootApplication(scanBasePackages = arrayOf("com.kaviddiss.example"))
class Application: SpringBootServletInitializer() {

    @Bean
    fun loadGeoNames(geoNameLoader: GeoNameLoader) = CommandLineRunner {
        geoNameLoader.loadGeoNamesToElasticsearch()
    }

    override fun configure(application: SpringApplicationBuilder): SpringApplicationBuilder {
        return application.sources(Application::class.java)
    }

}

fun main(args: Array<String>) {
    SpringApplication.run(Application::class.java, *args)
}