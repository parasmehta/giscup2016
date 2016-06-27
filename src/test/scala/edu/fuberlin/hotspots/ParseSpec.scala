package edu.fuberlin.hotspots

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
    trip = parse(line)
  }

  it should "have the correct vendorID" in {
    trip.vendorID shouldBe 2
  }
}
