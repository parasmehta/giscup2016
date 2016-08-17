package edu.fuberlin.hotspots

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Christian Windolf on 08.07.16.
  */
class SuperCellComputationSpec extends FlatSpec with Matchers{
  val superCellFactory = new SuperCellFactory(10)

  it should "return one SuperCell for a cell in the middle" in {
    superCellFactory.create((compose(5, 5, 5), 1)) should have size 1
  }

  it should "return the correct super cell id" in {
    superCellFactory.create((compose(5, 15, 15), 1))(0)._1 shouldEqual(compose(0, 10, 10))
  }

  it should "return two super cells for a left border cell" in {
    superCellFactory.create((compose(20, 15, 15), 1)) should have size 2
  }

  it should "return the correct main cell id" in {
    val map = superCellFactory.create((compose(10, 15, 15), 1)).toMap
    map(compose(0, 10, 10))._1 shouldEqual compose(10, 15, 15)
    map(compose(10, 10, 10))._1 shouldEqual compose(10, 15, 15)
  }

  it should "return two super cells for a right border cell" in {
    superCellFactory.create((compose(19, 15, 15), 1)) should have size 2
  }

  it should "return the correct main cell id for right border cell" in {
    val map = superCellFactory.create((compose(19, 15, 15), 1)).toMap
    all(map.values.map(_._1)) shouldEqual compose(19, 15, 15)
    map.keys.map(decompose) should contain (20, 10, 10)
    map.keys.map(decompose) should contain (10, 10, 10)
  }

  it should "return two super cells for an upper border cell" in {
    superCellFactory.create((compose(15, 10, 15), 1)) should have size 2
  }

  it should "return two super cells for a lower border cell" in {
    superCellFactory.create((compose(15, 59, 15), 1)) should have size 2
  }

  it should "find four cells for an edge cell" in {
    superCellFactory.create((compose(20, 20, 5), 1)) should have size 4
  }

  it should "find correct base cells" in {
    val map = superCellFactory.create((compose(20, 20, 5), 1)).toMap
    val cellIDs = map.keys.map(decompose)
    cellIDs should contain (20, 20, 0)
    cellIDs should contain (10, 20, 0)
    cellIDs should contain (10, 10, 0)
    cellIDs should contain (20, 10, 0)
    all(map.values.map(_._1)) shouldEqual compose(20, 20, 5)
  }

  it should "find 8 cells for a corner cell" in {
    superCellFactory.create((compose(20, 20, 20), 1)) should have size 8
  }

  it should "return eight cells for a corner cells" in {
    val map = superCellFactory.create((compose(20, 20, 20), 1)).toMap
    map should have size 8
    all(map.values.map(_._1)) shouldEqual compose(20, 20, 20)
    val cellIDs = map.keys map decompose
    cellIDs should contain (20, 20, 20)
    cellIDs should contain (10, 20, 20)
    cellIDs should contain (20, 10, 20)
    cellIDs should contain (10, 10, 20)
    cellIDs should contain (20, 20, 10)
    cellIDs should contain (10, 20, 10)
    cellIDs should contain (20, 10, 10)
    cellIDs should contain (10, 10, 10)
  }
}
