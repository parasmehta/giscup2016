package edu.fuberlin.hotspots

import scala.collection.mutable
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Christian Windolf on 11.07.16.
  */
class SuperCellSpec extends FlatSpec with Matchers {
  val map = new mutable.HashMap[Cellid, Int]
  for(x <- 1 to 10; y <- 1 to 10; t <- 1 to 10){
    map.put((x, y, t), 1)
  }
  val sc = new SuperCell(map.toMap, 8, (1L, 1L, 1L))

  it should "find 26 neighbours" in {
    sc.neighbours((5,5,5)) should have size 26
  }

  it should "have the correct values" in {
    all(sc.neighbours((5, 5, 5))) shouldBe 1
  }

  it should "find all 512 core cells" in {
    sc.coreCells should have size 512
  }
}
