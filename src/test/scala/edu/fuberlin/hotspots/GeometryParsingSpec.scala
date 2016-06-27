package edu.fuberlin.hotspots

import com.esri.core.geometry.Point
import GeoHelpers._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Christian Windolf on 27.06.16.
  */
class GeometryParsingSpec extends FlatSpec with Matchers{
  val src = getClass.getResource("/nyc-boroughs.geojson")

  it should "find a point in Manhattan" in {
    val pointInManhattan = new Point(-73.978148, 40.766037)
    boroughOf(pointInManhattan) shouldBe Some("Manhattan")
  }

  it should "not find a point from Berlin" in {
    val pointInBerlin = new Point(13.325553, 52.530886)
    boroughOf(pointInBerlin) shouldBe None
  }

  it should "find a point from Brooklyn" in {
    val pointInBrooklyn = new Point(-73.948659, 40.661467)
    boroughOf(pointInBrooklyn) shouldBe Some("Brooklyn")
  }

  it should "find a point from Staten Island" in {
    val pointInStatenIsland = new Point(-74.115569, 40.589857)
    boroughOf(pointInStatenIsland) shouldBe Some("Staten Island")
  }
}
