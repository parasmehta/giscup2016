package edu.fuberlin.hotspots

import org.joda.time.{DateTime, Duration}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * Created by Christian Windolf on 27.06.16.
  */
class ParseSpec extends FlatSpec with Matchers with BeforeAndAfter {
  var trip: Trip = null

  val line = "2,2015-01-15 19:05:39,2015-01-15 19:23:42,1,1.59," +
    "-73.993896484375,40.750110626220703,1,N,-73.974784851074219," +
    "40.750617980957031,1,12,1,0.5,3.25,0,0.3,17.05\n"

  before {
    trip = parseTrip(line)
  }

  it should "have the correct vendorID" in {
    trip.vendorID shouldBe 2
  }

  it should "have the correct pickup time" in {
    val duration = new Duration(new DateTime(2015, 1, 15, 19, 5), trip.pickup.time)
    val minutes = duration.getStandardMinutes
    minutes shouldBe 0
  }

  it should "have the correct dropoff time" in {
    val duration = new Duration(new DateTime(2015, 1, 15, 19, 23), trip.dropoff.time)
    val minutes = duration.getStandardMinutes
    minutes shouldBe 0
  }

  it should "have the correct pickup location" in {
    trip.pickup.location.getX shouldBe -73.9938 +- 0.0001
    trip.pickup.location.getY shouldBe 40.7501 +- 0.0001
  }

  it should "have the correct dropoff location" in {
    trip.dropoff.location.getX shouldBe -73.9747 +- 0.0001
    trip.dropoff.location.getY shouldBe 40.7506 +- 0.0001
  }

  it should "have the correct trip distance" in {
    trip.tripDistance shouldBe 1.59  +- 0.001
  }

  it should "have the correct passenger count" in {
    trip.passengerCount shouldBe 1
  }
}
