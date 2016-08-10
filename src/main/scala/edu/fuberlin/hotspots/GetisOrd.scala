package edu.fuberlin.hotspots

import java.lang.Math.sqrt

import org.apache.commons.math3.distribution.NormalDistribution

import org.apache.spark.rdd.RDD
import edu.fuberlin.hotspots.SparkHelpers._

import scala.collection.mutable.ListBuffer

/**
  * Created by Christian Windolf on 01.07.16.
  */
object GetisOrd {
  val superCellSize = 25

  def calculate(trips:RDD[Trip], cellSize:Any, timeSpan:Any):RDD[(Cellid, Double, Double)] = {
    val observations = trips.toCells(cellSize, timeSpan).cache
    val stdDev = observations.values.stdev
    val mean = observations.values.mean
    val count = observations.count
    val factory = new SuperCellFactory(superCellSize)
    val superCells = observations.flatMap(factory.create).mapValues(Seq(_)).reduceByKey(_ ++ _).map(c => new SuperCell(c._2, superCellSize, c._1))
    val zValue = zValueFunction(stdDev, mean, count)
    superCells.flatMap(zValue)
  }

  def zValueFunction(stdev:Double, mean:Double, n:Long):SuperCell => Seq[(Cellid, Double, Double)] = {
        val norm = new NormalDistribution()
    (superCell:SuperCell) => {
      val buffer = new ListBuffer[(Cellid, Double, Double)]
      for((cellid, passengerCount) <- superCell.coreCells){
        val neighbours = superCell.neighbours(cellid)
        val radicant = ((n * (neighbours.size + 1)) - Math.pow(neighbours.size + 1, 2.0)) / (n - 1)
        val denominator = stdev * sqrt(radicant)
        val numerator = neighbours.sum + passengerCount - (mean * (neighbours.size + 1))
        val zValue = numerator / denominator
        buffer.append((cellid, zValue, 1 - norm.cumulativeProbability(Math.abs(zValue))))
      }
      buffer
    }
  }
}
