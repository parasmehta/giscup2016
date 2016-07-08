package edu.fuberlin.hotspots

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * Created by Christian Windolf on 08.07.16.
  */
class SuperCellComputationSpec extends FlatSpec with Matchers with BeforeAndAfter{
  val superCellFactory = new SuperCellFactory(10)

  it should "return one SuperCell for a cell in the middle" in {
    superCellFactory.create(((-5, 5, 5), 1)) should have size 1
  }

  it should "return the correct base" in {
    superCellFactory.create(((-15, 15, 15), 1))(0)._2.base shouldEqual (-20, 10, 10)
  }

  it should "return the correct super cell id" in {
    superCellFactory.create(((-15, 15, 15), 1))(0)._1 shouldEqual(-2, 1, 1)
  }

  it should "return two super cells for a left border cell" in {
    superCellFactory.create(((-10, 15, 15), 1)) should have size 2
  }

  it should "return the correct main cell id" in {
    val map = superCellFactory.create(((-10, 15, 15), 1)).toMap
    map((-1, 1, 1)).base shouldEqual(-10, 10, 10)
    map((-2, 1, 1)).base shouldEqual(-20, 10, 10)
  }

  it should "return two super cells for a right border cell" in {
    superCellFactory.create(((-11, 15, 15), 1)) should have size 2
  }

  it should "return the correct main cell id for right border cell" in {
    val map = superCellFactory.create(((-11, 15, 15), 1)).toMap
    map((-2, 1, 1)).base shouldEqual((-20, 10, 10))
    map((-1, 1, 1)).base shouldEqual((-10, 10, 10))
  }

  it should "return two super cells for an upper border cell" in {
    superCellFactory.create(((-15, 10, 15), 1)) should have size 2
  }

  it should "return two super cells for a lower border cell" in {
    superCellFactory.create(((-15, 9, 15), 1)) should have size 2
  }

  it should "find four cells for an edge cell" in {
    superCellFactory.create(((-20, 20, 5), 1)) should have size 4
  }

  it should "return the correct main cell" in {
    val map = superCellFactory.create(((-20, 20, 5), 1)).toMap
    map((-2, 2, 0)).base shouldEqual((-20, 20, 0))
    map((-3, 2, 0)).base shouldEqual((-30, 20, 0))
    map((-2, 1, 0)).base shouldEqual((-20, 10, 0))
    map((-3, 1, 0)).base shouldEqual((-30, 10, 0))
  }

  it should "return eight cells for a corner cells" in {
    val map = superCellFactory.create(((-20, 20, 20), 1)).toMap
    map((-2, 2, 2)).base shouldEqual((-20, 20, 20))
    map((-3, 2, 2)).base shouldEqual((-30, 20, 20))
    map((-2, 1, 2)).base shouldEqual((-20, 10, 20))
    map((-3, 1, 2)).base shouldEqual((-30, 10, 20))
    map((-2, 2, 1)).base shouldEqual((-20, 20, 10))
    map((-3, 2, 1)).base shouldEqual((-30, 20, 10))
    map((-2, 1, 1)).base shouldEqual((-20, 10, 10))
    map((-3, 1, 1)).base shouldEqual((-30, 10, 10))
  }
}
