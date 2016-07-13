package edu.fuberlin.hotspots

import org.scalatest.{Ignore, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Created by Christian Windolf on 08.07.16.
  */
@Ignore
class GetisOrdWithoutHotspotsSpec extends SparkSpec with Matchers {
  def createTestData():Array[((Long, Long, Long), Int)] = {
    val data = new ListBuffer[((Long, Long, Long), Int)]()
    for(x <- 1 to 100; y <- 1 to 100; t <- 1 to 100) {
      //create a stdev of 0.5 by using two alternating values
      data.append(((x, y , t), t % 2))
    }
    data.toArray
  }

  it should "not return more items than the input" taggedAs(SparkSpec) in { f =>
    GetisOrd.calculate(f.context.parallelize(createTestData)).count.toInt should be < 100 * 100 * 100
  }

  /*
  it should "give every cell the same value" taggedAs(SparkSpec) in { f =>
    val results = GetisOrd.calculate(f.context.parallelize(createTestData)).collect.toMap
    all(results.values) shouldEqual 1.0 +- 0.1
  }
  */

  it should "only have unique cell identifiers" taggedAs(SparkSpec) in { f=>
    val results = GetisOrd.calculate(f.context.parallelize(createTestData)).collect.map(_._1)
    results.toSet.size shouldEqual results.size
  }
}
