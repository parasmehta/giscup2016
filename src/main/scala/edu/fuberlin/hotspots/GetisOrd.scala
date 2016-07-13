package edu.fuberlin.hotspots

import java.lang.Math.sqrt

import scala.collection.mutable
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Created by Christian Windolf on 01.07.16.
  */
object GetisOrd {
  def calculate(rawObservations:RDD[(Cellid, Int)]):RDD[(Cellid, Double)] = {
    val observations = rawObservations.cache
    val stdDev = observations.values.stdev
    val mean = observations.values.mean
    val count = observations.count
    val factory = new SuperCellFactory(25)
    val superCells = rawObservations.flatMap(factory.create).reduceByKey((a, b) => a ++ b).values.cache
    val zValue = zValueFunction(stdDev, mean, count)
    superCells.flatMap(zValue)
  }

  def zValueFunction(stdev:Double, mean:Double, n:Long):SuperCell => Seq[(Cellid, Double)] = {
    (superCell:SuperCell) => {
      val buffer = new ListBuffer[(Cellid, Double)]
      for((cellid, passengerCount) <- superCell.coreCells){
        val neighbours = superCell.neighbours(cellid)
        val radicant = ((n * (neighbours.size + 1)) - Math.pow(neighbours.size + 1, 2.0)) / (n - 1)
        val denominator = stdev * sqrt(radicant)
        val numerator = neighbours.sum + passengerCount - (mean * (neighbours.size + 1))
        val zValue = numerator / denominator
        buffer.append((cellid, zValue))
      }
      buffer.toSeq
    }
  }
}
