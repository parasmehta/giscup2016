package edu.fuberlin.hotspots

import java.lang.Math.{sqrt, abs, pow}

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

  /**
    * Calculates the p-values and z-scores for a grid of cells.
    * @param cells
    * @return
    */
  def calculate(cells:RDD[(Int, Int)], count:Long):RDD[(Int, Double, Double)] = {
    cells.cache()
    //Phase 2 (from cells to supercells)
    val factory = new SuperCellFactory(superCellSize)
    val superCells = cells.flatMap(factory.create).aggregateByKey(Seq[(Int, Int)]())(_ :+ _, _ ++ _)
      .map(c => new SuperCell(c._2, superCellSize, c._1))
    //End Phase 2

    //Phase 3 (from supercells to zscore)
    val sum = cells.values.sum
    val mean = sum.toDouble / count.toDouble
    val stdDev = math.sqrt((cells.values.map(p => p * p).sum / count) - (mean * mean))
    cells.unpersist()
    val norm = new NormalDistribution()
    val denominator = stdDev * sqrt(((count * 27) - 729) / (count - 1))
    superCells.flatMap(superCell => {
      val buffer = new ListBuffer[(Int, Double, Double)]
      for((cellid, passengerCount) <- superCell.coreCells){
        val neighbours = superCell.neighbours(cellid)
        val numerator = neighbours.sum - mean * 27
        val zValue = numerator / denominator
        /*
        It was not absolutely clear, how the p-value should be calculated (assuming which distribution and so on)
        We chose to stick with the PySal implementation of p-values of z-scores.
        That is 1 - norm.cdf(zScore)
         */
        buffer.append((compose(cellid), zValue, 1 - norm.cumulativeProbability(abs(zValue))))
      }
      buffer
    })
    //End Phase 3
  }
}
