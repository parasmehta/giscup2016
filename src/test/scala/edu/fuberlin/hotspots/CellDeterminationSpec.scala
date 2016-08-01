package edu.fuberlin.hotspots

import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Christian Windolf on 29.06.16.
  */
class CellDeterminationSpec extends FlatSpec with Matchers {
  val location = new Point(-73.956960, 40.794516, new DateTime(2015, 1, 30, 18, 59))
  val cellOf = cellDeterminationBuilder(0.001, 1)

  it should "have the correct long cell id" in {
    cellOf(location)._1 shouldBe -73956
  }

  it should "have the correct lat cell id" in {
    cellOf(location)._2 shouldBe 40794
  }

  it should "have the correct time cell" in {
    cellOf(location)._3 shouldBe 30
  }
}
