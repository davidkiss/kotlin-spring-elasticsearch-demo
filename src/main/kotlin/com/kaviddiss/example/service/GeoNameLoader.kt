package com.kaviddiss.example.service

import com.kaviddiss.example.model.GeoName
import com.kaviddiss.example.repo.GeoNameRepo
import org.elasticsearch.common.geo.GeoPoint
import org.slf4j.LoggerFactory
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate
import org.springframework.stereotype.Component
import java.nio.charset.Charset
import java.util.zip.ZipInputStream

@Component
class GeoNameLoader(val geoNameRepo: GeoNameRepo,
                    val elasticsearchTemplate: ElasticsearchTemplate)
{
    private val log = LoggerFactory.getLogger(this.javaClass)
    private val batchSize = 10_000

    fun loadGeoNamesToElasticsearch() {
        log.info("Loading geonames to Elasticsearch")
        // uncomment if you changed the GeoName fields:
        // reindexElasticsearch()

        geoNameRepo.deleteAll()
        val batch = mutableListOf<GeoName>()

        val geoNames = loadGeoNamesFromFile()
        if (geoNames != null) {
            geoNames.forEach(saveGeoNameBatchFunc(batch, batchSize))
            saveGeoNameBatch(batch)
        }
        log.info("Loading completed")
    }

    private fun reindexElasticsearch() {
        elasticsearchTemplate.deleteIndex(GeoName::class.java)
        elasticsearchTemplate.createIndex(GeoName::class.java)
        elasticsearchTemplate.putMapping(GeoName::class.java)
        elasticsearchTemplate.refresh(GeoName::class.java)
    }

    private fun saveGeoNameBatchFunc(batch: MutableList<GeoName>, batchSize: Int): (GeoName) -> Unit {
        return { geoName ->
            // only storing cities and landmarks:
            if (arrayOf("P", "S").contains(geoName.featureClass)) {
                batch.add(geoName)

                if (batch.size == batchSize) {
                    saveGeoNameBatch(batch)
                }
            }
        }
    }

    private fun saveGeoNameBatch(batch: MutableList<GeoName>) {
        geoNameRepo.save(batch)
        batch.clear()

        val count = geoNameRepo.count()
        log.info("Saved ${count} geonames")
    }

    private fun loadGeoNamesFromFile(): Sequence<GeoName>? {
        val geoNamesFile: String = "/geonames/CA.zip"
        log.info("Reading geonames from ${geoNamesFile}")
        // the CA.zip file was downloaded from http://download.geonames.org/export/dump/
        // and licensed under a Creative Commons Attribution 3.0 License, see http://creativecommons.org/licenses/by/3.0/
        val zipInputStream = ZipInputStream(javaClass.getResourceAsStream(geoNamesFile))
        var geoNameStream: Sequence<GeoName>? = null
        while (true) {
            val zipEntry = zipInputStream.getNextEntry()
            if (zipEntry == null) {
                break
            } else if (zipEntry.name.equals("CA.txt")) {
                geoNameStream = zipInputStream.bufferedReader(Charset.forName("UTF-8")).lineSequence().map { line -> lineToGeoName(line) }
                break
            }
        }
        log.info("Reading completed")
        return geoNameStream
    }

    private fun lineToGeoName(line: String): GeoName {
        val fields = line.split("\t")
        val geoName = GeoName(fields[0], fields[1], fields[2], fields[3], GeoPoint(fields[4].toDoubleOrNull() ?: 0.0, fields[5].toDoubleOrNull() ?: 0.0),
                fields[6], fields[7], fields[8], fields[14].toIntOrNull(), fields[15].toIntOrNull(), fields[17], fields[18])
        return geoName
    }

}