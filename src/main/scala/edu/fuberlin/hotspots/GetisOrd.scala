package edu.fuberlin.hotspots

import java.lang.Math.sqrt

import geotrellis.spark.io.index.zcurve.Z3

import scala.collection.mutable
import org.apache.spark.rdd.RDD

/**
  * Created by Christian Windolf on 01.07.16.
  */
object GetisOrd {
  def calculate(rawObservations:RDD[((Long, Long, Long), Int)]):RDD[((Long, Long, Long), Double)] = {
    val observations = rawObservations.setName("obervations").cache
    val context = observations.context
    val stdDev = observations.values.stdev
    val mean = observations.values.mean
    val count = observations.count
    def getNeighbours(cellid:(Long, Long, Long)): Array[((Long, Long, Long), Int)] = {
      val neighbours = new mutable.ListBuffer[(Long, Long, Long)]()
      for (x <- -1 to 1; y <- -1 to 1; t <- -1 to 1) {
        if (x != y || y != t || t != 0) {
          neighbours.append((x, y, t))
        }
      }
      neighbours.toList.map { (cell) =>  {
        (cell, observations.lookup(cell).toArray.lift(0))
      }}.collect({case (cellid, Some(v)) => (cellid, v)}).toArray
    }

    def zValue(cell:((Long, Long, Long),Int)): Double = {
      val (cellid, passengerCount) = cell
      val neighbours = getNeighbours(cellid)
      val radicant = (count * (neighbours.size + 1)) - Math.pow(neighbours.size + 1, 2.0)
      val denominator = mean * sqrt(radicant)
      val numerator = neighbours.map(_._2).reduce((a,b) => a + b) + passengerCount + (mean * (neighbours.size + 1))
      numerator / denominator
    }

    observations.map((cell) => (cell._1, zValue(cell)))
  }
}
