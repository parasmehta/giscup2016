package edu.fuberlin.hotspots

import com.esri.core.geometry.{Geometry, GeometryEngine, SpatialReference}

/**
  * Created by Christian Windolf on 27.06.16.
  */
object Helpers {
  implicit class RichGeometry(val geometry: Geometry) {
    val reference = SpatialReference.create(4326)
    def area2D = geometry.calculateArea2D
    def contains(other: Geometry) = GeometryEngine.contains(geometry, other, reference)
    def distance(other: Geometry) = GeometryEngine.distance(geometry, other, reference)
  }
}
