package edu.fuberlin.hotspots

import com.esri.core.geometry._
import GeometryEngine._
import spray.json._

/**
  * Created by Christian Windolf on 27.06.16.
  */
case class Feature(val id: Int, val borough: String, val geometry: Geometry)

object GeoJsonProtocol extends DefaultJsonProtocol{
  implicit object PolygonJsonFormat extends RootJsonFormat[Geometry] {
    val importFlags = GeoJsonImportFlags.geoJsonImportDefaults
    override def read(json: JsValue): Geometry = {
      geometryFromGeoJson(json.compactPrint, 0, Geometry.Type.Unknown).getGeometry
    }
    override def write(obj: Geometry): JsValue = JsObject()
  }

  implicit object FeatureJsonFormat extends RootJsonFormat[Feature]{
    override def read(json: JsValue): Feature = {
      val jsObj = json.asJsObject
      jsObj.getFields("id", "properties", "geometry") match {
        case Seq(JsNumber(id), JsObject(properties), geometry) =>
          Feature(id.toInt, properties("borough").toString.replaceAll("\"", ""), geometry.convertTo[Geometry])
        case _ =>
          val msg = s"Expected the fields 'id', 'borough' and 'geometry', but got ${jsObj.fields.keys.mkString(",")}"
          throw new InvalidGeoJsonException(msg)
      }
    }

    override def write(obj: Feature): JsValue = JsObject()
  }
}

object GeoHelpers {
  implicit class RichGeometry(val geometry: Geometry) {
    val reference = SpatialReference.create(4326)
    def area2D = geometry.calculateArea2D
    def contains(other: Geometry) = GeometryEngine.contains(geometry, other, reference)
    def distance(other: Geometry) = GeometryEngine.distance(geometry, other, reference)
  }

}

class InvalidGeoJsonException(message: String) extends RuntimeException(message)
