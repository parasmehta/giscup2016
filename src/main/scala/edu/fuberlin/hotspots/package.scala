package edu.fuberlin

import scala.util.matching.Regex

/**
  * Created by Christian Windolf on 24.06.16.
  */
package object hotspots {
  type Cellid = (Int, Int, Int)

  case class Trip(longitude:Double, latitude:Double, dayOfYear:Int, passengerCount: Int){
    def insideNYC = latitude >= 40.5d && latitude <= 40.9d && longitude >= -74.25d && longitude <= -73.7d
  }

  val dateRegex = new Regex("""^2015-(\d{2})-(\d{2}).*""")

  def parseTrip(line: String): Trip = {
    val fields = line.split(",")
    val dateRegex(m, d) = fields(2)
    val (month, day) = (m.toInt, d.toInt)
    val t = day + (month match {
      case 1 => 0 case 2 => 31 case 3 => 59 case 4 => 90 case 5 => 120 case 6 => 151
      case 7 => 181  case 8 => 212  case 9 => 243  case 10 => 273  case 11 => 304  case 12 => 334
    })
    val passengerCount = fields(3).toInt
    Trip(fields(9).toDouble, fields(10).toDouble, t, passengerCount)
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