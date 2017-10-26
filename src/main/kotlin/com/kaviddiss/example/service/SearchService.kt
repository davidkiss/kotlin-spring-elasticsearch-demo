package com.kaviddiss.example.service

import com.kaviddiss.example.model.GeoName
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.stereotype.Service

@Service
class SearchService (val geoNameService: GeoNameService){

    fun querySearch(query: String?, lat: Double?, lng: Double?, range: Double?, pageable: Pageable, hints: String?): Page<GeoName> {
        val geoNames = geoNameService.searchTravel(query, pageable, lat, lng, range)
        return geoNames
    }

}