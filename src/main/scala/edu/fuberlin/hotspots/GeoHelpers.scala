package edu.fuberlin.hotspots

import com.esri.core.geometry._
import spray.json._

/**
  * Created by Christian Windolf on 27.06.16.
  */
object GeoHelpers {
  case class Feature(val id: Int, val borough: String, val geometry: Geometry)

  implicit class RichGeometry(val geometry: Geometry) {
    val reference = SpatialReference.create(4326)
    def area2D = geometry.calculateArea2D
    def contains(other: Geometry) = GeometryEngine.contains(geometry, other, reference)
    def distance(other: Geometry) = GeometryEngine.distance(geometry, other, reference)
  }

  implicit object PolygonJsonFormat extends RootJsonFormat[Geometry] {
    val importFlags = GeoJsonImportFlags.geoJsonImportDefaults
    override def read(json: JsValue): Geometry = {
      val importOp = OperatorImportFromGeoJson.local()
      importOp.execute(importFlags, Geometry.Type.Polygon, json.compactPrint, null).getGeometry()
    }
    override def write(obj: Geometry): JsValue = JsObject()
  }

  implicit object FeatureJsonFormat extends RootJsonFormat[Feature]{
    override def read(json: JsValue): Feature = {
      val jsObj = json.asJsObject
      jsObj.getFields("id", "properties", "geometry") match {
        case Seq(JsNumber(id), JsObject(borough), geometry) =>
          Feature(id.toInt, borough("borough").toString.replaceAll("\"", ""), geometry.convertTo[Geometry])
        case _ =>
          val msg = s"Expected the fields 'id', 'borough' and 'geometry', but got ${jsObj.fields.keys.mkString(",")}"
          throw new InvalidGeoJsonException(msg)
      }
    }

    override def write(obj: Feature): JsValue = JsObject()
  }

  def readNYCBoroughs(src: scala.io.Source): Seq[Feature] = {
    val jsObj = src.mkString.parseJson.asJsObject
    val features = jsObj.fields("features").asInstanceOf[JsArray]
    features.elements.map(_.convertTo[Feature])
  }

  lazy val boroughs = readNYCBoroughs(scala.io.Source.fromURL(getClass().getResource("/nyc-boroughs.geojson")))

  def boroughOf(p: Point) = boroughs.find(_.geometry.contains(p)) match {
    case Some(b) => Some(b.borough) case _ => None
  }
}

class InvalidGeoJsonException(message: String) extends RuntimeException(message)
