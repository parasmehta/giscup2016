package edu.fuberlin.hotspots

import org.joda.time.DateTime
import org.scalatest.Matchers

import scala.collection.mutable.ListBuffer

/**
  * Created by Christian Windolf on 08.07.16.
  */
class GetisOrdWithoutHotspotsSpec extends SparkSpec with Matchers {
  def createTestData():Array[Trip] = {
    val data = new ListBuffer[Trip]()
    for(x <- 1 to 100; y <- 1 to 100; t <- 1 to 100) {
      //create a stdev of 0.5 by using two alternating values
      data.append(Trip(Point(x, y , new DateTime(2015, 1, 1,0,0).plusDays(t)), t % 2))
    }
    data.toArray
  }

  it should "not return more items than the input" taggedAs(SparkSpec) in { f =>
    GetisOrd.calculate(f.context.parallelize(createTestData), 1.0d, 1).count.toInt should be < 100 * 100 * 100
  }

  /*
  it should "give every cell the same value" taggedAs(SparkSpec) in { f =>
    val results = GetisOrd.calculate(f.context.parallelize(createTestData)).collect.toMap
    all(results.values) shouldEqual 1.0 +- 0.1
  }
  */

  it should "only have unique cell identifiers" taggedAs(SparkSpec) in { f=>
    val results = GetisOrd.calculate(f.context.parallelize(createTestData), 1.0d, 1).collect.map(_._1)
    results.toSet.size shouldEqual results.size
  }
}
