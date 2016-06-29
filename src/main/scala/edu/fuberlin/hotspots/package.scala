package edu.fuberlin

import java.text.SimpleDateFormat
import java.util.Locale

import com.esri.core.geometry.Point
import org.joda.time.DateTime
import spray.json._
import edu.fuberlin.hotspots.GeoHelpers._
import edu.fuberlin.hotspots.GeoJsonProtocol._

/**
  * Created by Christian Windolf on 24.06.16.
  */
package object hotspots {
  val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
  case class Trip(vendorID: Int,
             pickupTime: DateTime,
             dropoffTime: DateTime,
             passengerCount: Int,
             tripDistance: Double,
             pickupLocation: Point,
             dropoffLocation: Point
             )

  def parseTrip(line: String): Trip = {
    val fields = line.split(",")
    val vendorID = fields(0).toInt
    val pickupTime = new DateTime(timeFormat.parse(fields(1)))
    val dropoffTime = new DateTime(timeFormat.parse(fields(2)))
    val passengerCount = fields(3).toInt
    val tripDistance = fields(4).toDouble
    val pickupLocation = new Point(fields(5).toDouble, fields(6).toDouble)
    val dropoffLocation = new Point(fields(9).toDouble, fields(10).toDouble)
    Trip(vendorID, pickupTime, dropoffTime, passengerCount, tripDistance, pickupLocation, dropoffLocation)
  }

  def safe[I, O](f:I => O): I => Either[O, (I, Exception)] = {
    {(input) =>
      try {
        Left(f(input))
      } catch {
        case e: Exception => Right((input, e))
      }
    }
  }

  def readNYCBoroughs(src: scala.io.Source): Seq[Feature] = {
    val jsObj = src.mkString.parseJson.asJsObject
    val features = jsObj.fields("features").asInstanceOf[JsArray]
    features.elements.map(_.convertTo[Feature])
  }

  lazy val boroughs = readNYCBoroughs(scala.io.Source.fromURL(getClass().getResource("/nyc-boroughs.geojson")))

  def boroughOf(p: Point) = boroughs.find(_.geometry.contains(p)) match {
    case Some(b) => Some(b.borough)
    case None => None
  }
}