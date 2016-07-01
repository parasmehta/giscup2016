package edu.fuberlin.hotspots

import java.math.BigDecimal
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Christian Windolf on 29.06.16.
  */
object Submission {
  def main(args:Array[String]):Unit = {
    if(args.length < 4){
      println(s"Not enough arguments. Expected 4, got ${args.length}")
      printHelp
      return
    }

    val inputDirectory = args(0)

    val outputFile = args(1)

    if(inputDirectory.startsWith("hdfs") || outputFile.startsWith("hdfs")) {
      println("HDFS is not supported yet")
      return
    }

    val gridSize = try {new BigDecimal(args(2))} catch {
      case e:NumberFormatException => {
        println(s"'${args(2)}' is not a valid grid size")
        printHelp
        return
      }
    }

    val timeSpan = try {args(3).toInt} catch {
      case e:NumberFormatException => {
        println(s"Invalid values for day. '${args(3)}' is cannot be converted to an integer")
        printHelp
        return
      }
    }

    val sample = if(args.length >= 5) args(4).toDouble else 1
    submit(inputDirectory, outputFile, gridSize, timeSpan, sample)
  }

  def printHelp = {
    println("How to use it:")
    println("  spark-submit --class edu.fuberlin.hotspots.Submission --master someMasterUrl \\")
    println("  path/to/jar \\")
    println("  {hdfs,file,}://path/to/input_directory \\")
    println("  {hdfs,file,}://path/to/output_file \\")
    println("  {spatial gridsize in degrees (0.001 for example)}\\")
    println("  {temporal gridsize in days} (1 for example)")
  }

  def submit(inputDir:String, outputFile:String, gridSize:BigDecimal, timeSpan:Int, sample:Double):Unit = {
    val conf = new SparkConf().setAppName("Fu-Berlin")
    val sc = new SparkContext(conf)
    val cellOf = cellsFor(gridSize, timeSpan)
    val taxiData = if(sample == 1) sc.textFile(inputDir) else sc.textFile(inputDir).sample(true, sample)
    val trips = taxiData.map(skipErrors(parseTrip)).collect({case Some(t) => t})
    val tripsWithCells = trips.map({(t) => (cellOf(t.pickup), t.passengerCount)})
    val reducedCells = tripsWithCells.reduceByKey((a,b) => a + b)
    reducedCells.map {case ((x,y,t), passengers) => s"$x, $y, $t, $passengers"}.saveAsTextFile(outputFile)
  }
}
