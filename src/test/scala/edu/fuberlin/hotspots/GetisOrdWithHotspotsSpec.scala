package edu.fuberlin.hotspots

import java.lang.Math.{pow, sqrt}

import org.scalatest.Matchers

import scala.collection.mutable.ListBuffer

/**
  * Created by Christian Windolf on 06.07.16.
  */
class GetisOrdWithHotspotsSpec extends SparkSpec with Matchers {
  def dist(p1:(Long, Long, Long), p2:(Long, Long, Long)) = {
    sqrt(pow(p1._1 - p2._1, 2) + pow(p1._2 - p2._2, 2) + pow(p1._3 - p2._3, 2))
  }

  def createTestData():Array[((Long, Long, Long), Int)] = {
    val data = new ListBuffer[((Long, Long, Long), Int)]();
    //default 20
    //one hotspt at 10,10,10
    // big hotspot at 65,35,50
    for(x <- 1 to 100; y <- 1 to 100; t <- 1 to 100) {
      data.append((x,y,t) match {
        case (10,10,10) => ((10,10,10),40)
        case (x,y,t) if((60 to 70 contains x) && (30 to 40 contains y) && (45 to 55 contains t)) => {
          ((x,y,t), 35 - dist((x,y,t), (65,35, 50)).toInt)
        }
        case (x,y,t) => ((x,y,t), 20)
      })
    }
    data.toArray
  }

  it should "find the second hottest zone" in { f =>
    val testRDD = f.context.parallelize(createTestData())
    val resultRDD = GetisOrd.calculate(testRDD).cache
    val results = resultRDD.collect.toMap
    val mean = resultRDD.values.mean
    results((10, 10, 10)) should be > mean
  }

  it should "should be able to determine which spot is hotter" in { f =>
    val testRDD = f.context.parallelize(createTestData())
    val results = GetisOrd.calculate(testRDD).collect.toMap
    results((65, 35, 50)) should be > results((10,10,10))
  }
}

