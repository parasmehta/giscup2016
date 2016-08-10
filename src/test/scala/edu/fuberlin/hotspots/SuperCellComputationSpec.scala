package edu.fuberlin.hotspots

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Christian Windolf on 08.07.16.
  */
class SuperCellComputationSpec extends FlatSpec with Matchers{
  val superCellFactory = new SuperCellFactory(10)

  it should "return one SuperCell for a cell in the middle" in {
    superCellFactory.create((compose(-74205, 40505, 5), 1)) should have size 1
  }

  it should "return the correct super cell id" in {
    superCellFactory.create((compose(-74205, 40515, 15), 1))(0)._1 shouldEqual(compose(-74210, 40510, 10))
  }

  it should "return two super cells for a left border cell" in {
    superCellFactory.create((compose(-74200, 40515, 15), 1)) should have size 2
  }

  it should "return the correct main cell id" in {
    val map = superCellFactory.create((compose(-74010, 40515, 15), 1)).toMap
    map(compose(-74020, 40510, 10))._1 shouldEqual compose(-74010, 40515, 15)
    map(compose(-74010, 40510, 10))._1 shouldEqual compose(-74010, 40515, 15)
  }

  it should "return two super cells for a right border cell" in {
    superCellFactory.create((compose(-74011, 40515, 15), 1)) should have size 2
  }

  it should "return the correct main cell id for right border cell" in {
    val map = superCellFactory.create((compose(-74011, 40515, 15), 1)).toMap
    all(map.values.map(_._1)) shouldEqual compose(-74011, 40515, 15)
    map.keys.map(decompose) should contain (-74020, 40510, 10)
    map.keys.map(decompose) should contain (-74010, 40510, 10)
  }

  it should "return two super cells for an upper border cell" in {
    superCellFactory.create((compose(-74015, 40510, 15), 1)) should have size 2
  }

  it should "return two super cells for a lower border cell" in {
    superCellFactory.create((compose(-74015, 4059, 15), 1)) should have size 2
  }

  it should "find four cells for an edge cell" in {
    superCellFactory.create((compose(-74020, 40520, 5), 1)) should have size 4
  }

  it should "find correct base cells" in {
    val map = superCellFactory.create((compose(-74020, 40520, 5), 1)).toMap
    map.keys.map(decompose) should contain (-74020, 40520, 0)
    map.keys.map(decompose) should contain (-74030, 40520, 0)
    map.keys.map(decompose) should contain (-74030, 40510, 0)
    map.keys.map(decompose) should contain (-74020, 40510, 0)
    all(map.values.map(_._1)) shouldEqual compose(-74020, 40520, 5)
  }

  it should "find 8 cells for a corner cell" in {
    superCellFactory.create((compose(-74020, 40520, 20), 1)) should have size 8
  }

  it should "return eight cells for a corner cells" in {
    val map = superCellFactory.create((compose(-74020, 40520, 20), 1)).toMap
    map should have size 8
    all(map.values.map(_._1)) shouldEqual compose(-74020, 40520, 20)
    map.keys.map(decompose) should contain (-74020, 40520, 20)
    map.keys.map(decompose) should contain (-74030, 40520, 20)
    map.keys.map(decompose) should contain (-74020, 40510, 20)
    map.keys.map(decompose) should contain (-74030, 40510, 20)
    map.keys.map(decompose) should contain (-74020, 40520, 10)
    map.keys.map(decompose) should contain (-74030, 40520, 10)
    map.keys.map(decompose) should contain (-74020, 40510, 10)
    map.keys.map(decompose) should contain (-74030, 40510, 10)
  }
}
