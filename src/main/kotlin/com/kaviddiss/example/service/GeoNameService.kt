package com.kaviddiss.example.service

import com.kaviddiss.example.model.GeoName
import com.kaviddiss.example.repo.GeoNameRepo
import org.elasticsearch.common.unit.DistanceUnit
import org.elasticsearch.index.query.QueryBuilders.*
import org.elasticsearch.index.query.SimpleQueryStringBuilder
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder
import org.springframework.stereotype.Service

@Service
class GeoNameService(val geoNameRepo: GeoNameRepo)
{
    val log = LoggerFactory.getLogger(this.javaClass)

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
}