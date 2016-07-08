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
      data.append(((x, y , t), 1))
    }
    data.toArray
  }

  it should "not return more items than the input" in { f =>
    GetisOrd.calculate(f.context.parallelize(createTestData)).count shouldBe <=(10000)
  }

  it should "give every cell the same value" in { f =>
    val results = GetisOrd.calculate(f.context.parallelize(createTestData)).collect.toMap
    for(x <- 2 to 9; y <- 2 to 9; t <- 2 to 9){
      results((x, y, t)) shouldEqual 1.0 +- 0.1
    }
  }
}
