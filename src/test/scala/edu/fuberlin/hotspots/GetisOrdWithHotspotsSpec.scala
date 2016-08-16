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

  val composer = new Composer(0.001)

  def createTestData():Array[(Int, Int)] = {
    val data = new ListBuffer[(Int, Int)]();
    //default 20
    //one hotspt at -74240,40510,10
    // big hotspot at 74185,40535,50
    for(x <- -74250 to -74150; y <- 40500 to 40600; t <-0 until 100) {
      data.append((x,y,t) match {
        case (-74240, 40510, 10) => (composer.compose(-74240, 40510, 10), 40)
        case (x,y,t) if((-74190 to -74180 contains x) && (40530 to 40540 contains y) && (45 to 55 contains t)) => {
          (composer.compose(x,y, t), 35 - dist((x,y,t), (-74185, 40535, 50)).toInt)
        }
        case (x, y, t) => (composer.compose(x,y, t), 20)
      })
    }
    data.toArray
  }

  it should "find the second hottest zone" in { f =>
    val testRDD = f.context.parallelize(createTestData())
    val resultRDD = GetisOrd.calculate(testRDD, composer).cache
    val results = resultRDD.map(c => (c._1, (c._2, c._3))).collect.toMap
    val mean = resultRDD.map(_._2).mean
    results((-74240, 40510, 10))._1 should be > mean
  }

  it should "should be able to determine which spot is hotter" in { f =>
    val testRDD = f.context.parallelize(createTestData())
    val results = GetisOrd.calculate(testRDD, composer).map(c => (c._1, (c._2, c._3))).collect.toMap
    results((-74185, 40535, 50))._1 should be > results((-74240,40510,10))._1
  }
}

