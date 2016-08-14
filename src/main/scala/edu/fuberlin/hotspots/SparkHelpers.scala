package edu.fuberlin.hotspots

import java.math.BigDecimal

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Christian Windolf on 04.07.16.
  */
object SparkHelpers {
  implicit class RichContext(sc:SparkContext) {
    def loadTaxi(inputDir:String): RDD[Trip] = {
      val taxiData = sc.textFile(inputDir)
      val trips = taxiData.map(skipErrors(parseTrip)).collect({case Some(t) => t})
      trips.filter(t => t.insideNYC)
    }

    def debugLoadTaxi(inputDir:String): RDD[Either[Trip,(String, Exception)]] = {
      val taxiData = sc.textFile(inputDir)
      taxiData.map(saveErrors(parseTrip))
    }
  }

  implicit class RichErrors(rdd:RDD[Either[Trip,(String, Exception)]]){
    def errorRate:Double = {
      rdd.map(_.isLeft).countByValue.get(false).get.toDouble / rdd.count.toDouble
    }

    def errors = rdd.collect({case Right(t) => t})

    def errorStat = rdd.errors.map(_._2.getClass.getSimpleName).countByValue
  }

  implicit class RichTrips(trips:RDD[Trip]) {
    def toCells(gs:Any, ts:Any):RDD[(Int, Int)] = {
      val gridSize = gs match{ case s:String => s.toDouble case d:Double => d}
      val timeSpan = ts match{case s:String => s.toInt case i:Int => i}
      val cellsWithPassengers = trips.map((t) =>{
        (compose((t.longitude / gridSize).toInt,
          (t.latitude / gridSize).toInt,
          t.dayOfYear / timeSpan),
          t.passengerCount)
      })
      cellsWithPassengers.reduceByKey((a,b) => a + b)
    }
  }
}
