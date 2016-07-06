package edu.fuberlin.hotspots

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Christian Windolf on 04.07.16.
  */
object SparkHelpers {
  implicit class RichContext(sc:SparkContext) {
    private def load(inputDir:String, sample:Double): RDD[String] = {
      if(sample == 1) sc.textFile(inputDir) else sc.textFile(inputDir).sample(true, sample)
    }

    def loadTaxi(inputDir:String, sample:Double=1): RDD[Trip] = {
      val taxiData = load(inputDir, sample)
      val trips = taxiData.map(skipErrors(parseTrip)).collect({case Some(t) => t})
      trips.filter({(t) => boroughOf(t.pickup.location).isDefined})
    }

    def debugLoadTaxi(inputDir:String, sample:Double=1): RDD[Either[Trip,(String, Exception)]] = {
      val taxiData = load(inputDir, sample)
      taxiData.map(saveErrors(parseTrip))
    }
  }

  implicit class RichErrors(rdd:RDD[Either[Trip,(String, Exception)]]){
    def errorRate:Double = {
      rdd.map(_.isLeft).countByValue.get(false).get.toDouble / rdd.count.toDouble
    }

    def errors = rdd.collect({case t if t.isRight => t.right.get})

    def errorStat = rdd.errors.map(_._2.getClass).countByValue
  }
}
