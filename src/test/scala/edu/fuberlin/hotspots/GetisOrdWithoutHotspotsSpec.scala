package edu.fuberlin.hotspots

import org.scalatest.Matchers

import scala.collection.mutable.ListBuffer

/**
  * Created by Christian Windolf on 08.07.16.
  */
class GetisOrdWithoutHotspotsSpec extends SparkSpec with Matchers {
  val composer = new Composer(0.001)
  def createTestData():Array[(Int, Int)] = {
    val data = new ListBuffer[(Int, Int)]()
    for(x <- -74250 until -74150; y <- 40500 until 40600; t <-0 until 100) {
      //create a stdev of 0.5 by using two alternating values
      data.append((composer.compose(x, y , t), t % 2))
    }
    data.toArray
  }

  it should "not return more items than the input" taggedAs(SparkSpec) in { f =>
    GetisOrd.calculate(f.context.parallelize(createTestData), composer).count.toInt should be <= 100 * 100 * 100
  }

  it should "only have unique cell identifiers" taggedAs(SparkSpec) in { f=>
    val results = GetisOrd.calculate(f.context.parallelize(createTestData), composer).collect.map(_._1)
    results.toSet.size shouldEqual results.size
  }
}
