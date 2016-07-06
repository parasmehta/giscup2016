package edu.fuberlin

import java.text.SimpleDateFormat
import java.util.Locale

import com.esri.core.geometry.Point
import org.joda.time.{DateTime, Duration}
import spray.json._
import edu.fuberlin.hotspots.GeoHelpers._
import edu.fuberlin.hotspots.GeoJsonProtocol._

/**
  * Created by Christian Windolf on 24.06.16.
  */
package object hotspots {
  case class STPoint(location: Point, time: DateTime)

  def cellsFor(cellSize:BigDecimal, timeStep: Int):STPoint => (Int, Int, Int) = {
    (point:STPoint) => {
      ((point.location.getX/cellSize).toInt, (point.location.getY/cellSize).toInt, point.time.getDayOfYear)
    }
  }

  case class Trip(vendorID: Int, pickup: STPoint, dropoff: STPoint, passengerCount: Int, tripDistance: Double) {
    if(pickup.time.isAfter(dropoff.time)) {
      throw new IllegalArgumentException(s"pickup time at ${pickup.time} was before dropoff time at ${dropoff.time}")
    }
    if(new Duration(pickup.time, dropoff.time).getStandardHours > 4) {
      throw new IllegalArgumentException(s"There were more than four hours between pickup and dropoff time")
    }
  }

  def parseTrip(line: String): Trip = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    val fields = line.split(",")
    val vendorID = fields(0).toInt
    val pickup = new STPoint(new Point(fields(5).toDouble, fields(6).toDouble), new DateTime(format.parse(fields(1))))
    val dropoff = new STPoint(new Point(fields(9).toDouble, fields(10).toDouble), new DateTime(format.parse(fields(2))))
    val passengerCount = fields(3).toInt
    val tripDistance = fields(4).toDouble
    Trip(vendorID, pickup, dropoff, passengerCount, tripDistance)
  }

  def saveErrors[I, O](f:I => O): I => Either[O, (I, Exception)] = {
    {(input) =>
      try {
        Left(f(input))
      } catch {
        case e: Exception => Right((input, e))
      }
    }
  }

  def skipErrors[I, O](f: I => O): I => Option[O] = {
    {(input) =>
      try{
        Some(f(input))
      } catch{case e:Exception => None}
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