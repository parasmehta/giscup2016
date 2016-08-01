package edu.fuberlin.hotspots

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

/**
  * Created by Christian Windolf on 11.07.16.
  */
class GetisOrdSpec extends FlatSpec with Matchers {
  val map = new mutable.HashMap[Cellid, Int]
  for(x <- 0 to 5; y <- 0 to 5; t <- 0 to 5){
    map.put((x, y, t), 1)
  }
  val sc = new SuperCell(map.toMap, 4, (1L, 1L, 1L))

  val zValues = GetisOrd.zValueFunction(.5d, .5d, 216)

  it should "calculate 'only' 64 values" in {
    zValues(sc) should have size 64
  }
}
