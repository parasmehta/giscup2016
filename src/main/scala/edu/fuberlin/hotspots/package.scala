package edu.fuberlin

import java.text.SimpleDateFormat
import java.util.Locale

import org.joda.time.{DateTime, Duration}

/**
  * Created by Christian Windolf on 24.06.16.
  */
package object hotspots {
  case class Point(longitude: Double, latitude:Double, time: DateTime){
    def insideNYC = latitude >= 40.5d && latitude <= 40.9d && longitude >= -74.25d && longitude <= -73.7
  }

  type Cellid = (Long, Long, Long)

  def cellDeterminationBuilder(cellSize:BigDecimal, timeStep: Int):Point => (Long, Long, Long) = {
    (point:Point) => {
      ((point.longitude/cellSize).toLong, (point.latitude/cellSize).toLong, point.time.getDayOfYear.toLong / timeStep)
    }
  }

  case class Trip(dropoff: Point, passengerCount: Int)

  def parseTrip(line: String): Trip = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    val fields = line.split(",")
    val dropoff = Point(fields(9).toDouble, fields(10).toDouble, new DateTime(format.parse(fields(2))))
    val passengerCount = fields(3).toInt
    Trip(dropoff, passengerCount)
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
}