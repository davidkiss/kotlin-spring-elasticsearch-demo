package com.kaviddiss.example.web

import com.kaviddiss.example.service.SearchService
import org.springframework.data.domain.Pageable
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v1")
class SearchController(val searchService: SearchService)
{

    @GetMapping("/search/query")
    fun querySearch(@RequestParam(value = "query", required = false) query: String?,
                    @RequestParam(value = "lat", required = false) lat: Double?,
                    @RequestParam(value = "lng", required = false) lng: Double?,
                    @RequestParam(value = "range", required = false) range: Double?,
                    @RequestParam(value = "hints", required = false) hints: String?,
                    pageable: Pageable)
            = searchService.querySearch(query, lat, lng, range, pageable, hints)
}