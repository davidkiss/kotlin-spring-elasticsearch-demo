package com.kaviddiss.example.repo

import com.kaviddiss.example.model.GeoName
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository

interface GeoNameRepo : ElasticsearchRepository<GeoName, String> {
}