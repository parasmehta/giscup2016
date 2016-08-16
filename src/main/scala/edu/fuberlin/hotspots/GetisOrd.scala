package edu.fuberlin.hotspots

import java.lang.Math.{sqrt, abs}

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Created by Christian Windolf on 01.07.16.
  */
object GetisOrd {
  /**
    * The size of a supercell in one dimension.
    * As we have three dimensions, the size of a supercell is 25 * 25 * 25 = 15625.
    * This the possible number of core cells. Then there are also the buffer cells.
    * So in the end, we might end up with 27 * 27 * 27 = 19683 cells in one supercell.
    */
  val superCellSize = 25

  def calculate(cells:RDD[(Int, Int)], composer:Composer):RDD[(Cellid, Double, Double)] = {
    cells.cache()
    val stdDev = cells.values.stdev
    val mean = cells.values.mean
    val count = cells.count
    val norm = new NormalDistribution()
    val factory = new SuperCellFactory(superCellSize, composer)
    val superCells = cells.flatMap(factory.create).aggregateByKey(Seq[(Int, Int)]())(_ :+ _, _ ++ _)
      .map(c => new SuperCell(c._2, superCellSize, c._1, composer))
    cells.unpersist()
    superCells.flatMap(superCell => {
      val buffer = new ListBuffer[(Cellid, Double, Double)]
      for((cellid, passengerCount) <- superCell.coreCells){
        val neighbours = superCell.neighbours(cellid)
        val radicant = ((count * neighbours.size) - Math.pow(neighbours.size, 2.0)) / (count - 1)
        val denominator = stdDev * sqrt(radicant)
        val numerator = neighbours.sum - (mean * neighbours.size)
        val zValue = numerator / denominator
        /*
        It was not absolutely clear, how the p-value should be calculated (assuming which distribution and so on)
        We chose to stick with the PySal implementation of p-values of z-scores.
        That is 1 - norm.cdf(zScore)
         */
        buffer.append((cellid, zValue, 1 - norm.cumulativeProbability(abs(zValue))))
      }
      buffer
    })
  }
}
