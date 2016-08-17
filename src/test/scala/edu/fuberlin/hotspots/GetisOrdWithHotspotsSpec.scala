package edu.fuberlin.hotspots

import java.lang.Math.{pow, sqrt}

import org.apache.spark.broadcast.Broadcast
import org.scalatest.Matchers

import scala.collection.mutable.ListBuffer

/**
  * Created by Christian Windolf on 06.07.16.
  */
class GetisOrdWithHotspotsSpec extends SparkSpec with Matchers {
  def dist(p1:(Long, Long, Long), p2:(Long, Long, Long)) = {
    sqrt(pow(p1._1 - p2._1, 2) + pow(p1._2 - p2._2, 2) + pow(p1._3 - p2._3, 2))
  }

  def createTestData():Array[(Int, Int)] = {
    val data = new ListBuffer[(Int, Int)]();
    //default 20
    //one hotspt at 110,10,10
    // big hotspot at 165,35,50
    for(x <- 100 until 200; y <- 0 until 100; t <-0 until 100) {
      data.append((x,y,t) match {
        case (110, 10, 10) => (compose(110, 10, 10), 40)
        case (x,y,t) if((160 to 170 contains x) && (30 to 40 contains y) && (45 to 55 contains t)) => {
          (compose(x,y, t), 35 - dist((x,y,t), (165, 35, 50)).toInt)
        }
        case (x, y, t) => (compose(x,y, t), 20)
      })
    }
    data.toArray
  }

  it should "find the second hottest zone" in { f =>
    val testRDD = f.context.parallelize(createTestData())
    val resultRDD = GetisOrd.calculate(testRDD).cache
    val results = resultRDD.map(c => (c._1, (c._2, c._3))).collect.toMap
    val mean = resultRDD.map(_._2).mean
    results(compose(110, 10, 10))._1 should be > mean
  }

  it should "should be able to determine which spot is hotter" in { f =>
    val testRDD = f.context.parallelize(createTestData())
    val results = GetisOrd.calculate(testRDD).map(c => (c._1, (c._2, c._3))).collect.toMap
    results(compose(165, 35, 50))._1 should be > results(compose(110, 10, 10))._1
  }
}
