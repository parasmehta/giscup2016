package edu.fuberlin.hotspots

import org.scalatest.Matchers

import scala.collection.mutable.ListBuffer

/**
  * Created by Christian Windolf on 08.07.16.
  */
class GetisOrdWithoutHotspotsSpec extends SparkSpec with Matchers {
  def createTestData():Array[(Int, Int)] = {
    val data = new ListBuffer[(Int, Int)]()
    for(x <- 0 until 100; y <- 0 until 100; t <- 0 until 100) {
      //create a stdev of 0.5 by using two alternating values
      data.append((compose(x, y , t), t % 2))
    }
    data.toArray
  }

  it should "not return more items than the input" taggedAs(SparkSpec) in { f =>
    GetisOrd.calculate(f.context.parallelize(createTestData)).count.toInt should be <= 100 * 100 * 100
  }

  it should "only have unique cell identifiers" taggedAs(SparkSpec) in { f =>
    val results = GetisOrd.calculate(f.context.parallelize(createTestData)).collect.map(_._1)
    results.toSet.size shouldEqual results.size
  }
}
