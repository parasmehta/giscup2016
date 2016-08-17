package edu.fuberlin.hotspots

import java.util

import scala.collection.mutable
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Christian Windolf on 11.07.16.
  */
class SuperCellSpec extends FlatSpec with Matchers {
  val buffer = new mutable.ListBuffer[(Int, Int)]
  for(x <- 10 to 20; y <- 0 to 10; t <- 0 until 10){
    buffer.append((compose(x, y, t), 1))
  }
  val sc = new SuperCell(buffer, 8, compose(11, 1, 1))

  it should "find 26 neighbours" in {
    sc.neighbours((15, 5, 5)) should have size 27
  }

  it should "have the correct values" in {
    all(sc.neighbours((5, 5, 5))) shouldBe 1
  }

  it should "find all 512 core cells" in {
    sc.coreCells should have size 512
  }
}
