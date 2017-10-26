package com.kaviddiss.example.service

import com.kaviddiss.example.model.GeoName
import com.kaviddiss.example.repo.GeoNameRepo
import org.elasticsearch.common.geo.GeoPoint
import org.elasticsearch.common.unit.DistanceUnit
import org.elasticsearch.index.query.QueryBuilders.*
import org.elasticsearch.index.query.SimpleQueryStringBuilder
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder
import org.springframework.stereotype.Service
import java.nio.charset.Charset
import java.util.zip.ZipInputStream

@Service
class GeoNameService(val geoNameRepo: GeoNameRepo,
                     val elasticsearchTemplate: ElasticsearchTemplate)
{
    val log = LoggerFactory.getLogger(this.javaClass)
    val batchSize = 10_000

    fun initElasticSearchDataset() {
        initGeoNameData()
    }

    private fun initGeoNameData() {
        // in case you change the GeoName fields:
        // reindexElasticsearch()

        geoNameRepo.deleteAll()
        val batch = mutableListOf<GeoName>()

        val geoNames = loadGeoNames()
        if (geoNames != null) {
            geoNames.forEach(saveGeoNameBatchFunc(batch, batchSize))
            saveGeoNameBatch(batch)
        }
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

    fun searchTravel(query: String?, pageable: Pageable?, lat: Double?, lng: Double?, range: Double?): Page<GeoName> {
        val searchQuery = createNativeSearchQuery(query, lat, lng, range, pageable)
        val page = geoNameRepo.search(searchQuery)
        val fixedPage = fixPageForSerialization(page)
        return fixedPage
    }

    private fun createNativeSearchQuery(query: String? = null, lat: Double? = null, lng: Double? = null, range: Double? = 1000.0, pageable: Pageable?): NativeSearchQuery? {
        val boolQuery = boolQuery()
        if (query != null) {
            boolQuery.must(simpleQueryStringQuery(query).field("name").defaultOperator(SimpleQueryStringBuilder.Operator.AND))
        }
        if (lat != null && lng != null && range != null) {
            boolQuery.must(geoDistanceQuery("location").lat(lat).lon(lng).distance(range, DistanceUnit.METERS))
        }
        return NativeSearchQueryBuilder()
                .withPageable(pageable)
                .withFilter(boolQuery)
                .build();
    }

    private fun <T> fixPageForSerialization(page: Page<T>): Page<T> =
            // workaround for NPE in FacetedPageImpl#processAggregations() line 89:
            page.map { item -> item }

    fun loadGeoNames(): Sequence<GeoName>? {
        // download CA.zip file from http://download.geonames.org/export/dump/
        val zipInputStream = ZipInputStream(javaClass.getResourceAsStream("/geonames/CA.zip"))
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
        return geoNameStream
    }

    fun lineToGeoName(line: String): GeoName {
        val fields = line.split("\t")
        val geoName = GeoName(fields[0], fields[1], fields[2], fields[3], GeoPoint(fields[4].toDoubleOrNull() ?: 0.0, fields[5].toDoubleOrNull() ?: 0.0),
                fields[6], fields[7], fields[8], fields[14].toIntOrNull(), fields[15].toIntOrNull(), fields[17], fields[18])
        return geoName
    }
}