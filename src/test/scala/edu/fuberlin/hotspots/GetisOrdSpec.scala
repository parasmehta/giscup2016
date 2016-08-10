package edu.fuberlin.hotspots

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

/**
  * Created by Christian Windolf on 11.07.16.
  */
class GetisOrdSpec extends FlatSpec with Matchers {
  val buffer = new mutable.ListBuffer[(Int, Int)]
  for(x <- -74250 to -74240 ; y <- 40500 to 40505; t <- 0 to 5){
    buffer.append((compose(x, y, t), 1))
  }
  val sc = new SuperCell(buffer, 4, compose(-74249, 40501, 1))

  val zValues = GetisOrd.zValueFunction(.5d, .5d, 216)

  it should "calculate 'only' 64 values" in {
    zValues(sc) should have size 64
  }
}
