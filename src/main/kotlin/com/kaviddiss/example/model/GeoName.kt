package com.kaviddiss.example.model

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.node.ObjectNode
import org.elasticsearch.common.geo.GeoPoint
import org.springframework.data.elasticsearch.annotations.Document
import org.springframework.data.elasticsearch.annotations.GeoPointField

@Document(indexName = "geoname_idx", type = "geoname")
data class GeoName(val id: String, val name: String, val asciiname: String, val alternatenames: String,
                   @GeoPointField @JsonSerialize(using = GeoPointSerializer::class) @JsonDeserialize(using = GeoPointDeserializer::class) val location: GeoPoint,
                   val featureClass: String, val featureCode: String, val countryCode: String, val population: Int? = 0, val elevation: Int? = 0,
                   val timezone: String, val modificationDate: String)

class GeoPointSerializer: JsonSerializer<GeoPoint>() {
    override fun serialize(value: GeoPoint, jgen: JsonGenerator, serializers: SerializerProvider?) {
        jgen.writeStartObject()
        jgen.writeNumberField("lat", value.lat)
        jgen.writeNumberField("lon", value.lon)
        jgen.writeEndObject()
    }
}

class GeoPointDeserializer: JsonDeserializer<GeoPoint>() {
    override fun deserialize(jp: JsonParser, ctxt: DeserializationContext): GeoPoint {
        val objectNode: ObjectNode = jp.readValueAsTree()
        return GeoPoint(objectNode.get("lat").doubleValue(), objectNode.get("lon").doubleValue())
    }
}