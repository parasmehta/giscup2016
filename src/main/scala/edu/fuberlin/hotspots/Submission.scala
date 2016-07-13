package edu.fuberlin.hotspots

import java.math.BigDecimal

import org.apache.spark.{SparkConf, SparkContext}
import SparkHelpers._
import org.apache.spark.rdd.RDD

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

    val sample = args.lift(4).getOrElse("1").toDouble
    val conf = new SparkConf().setAppName("Fu-Berlin")
    val sc = new SparkContext(conf)
    submit(sc, inputDirectory, outputFile, gridSize, timeSpan, sample)
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

  def submit(sc:SparkContext,
             inputDir:String,
             outputDir:String,
             gridSize:BigDecimal,
             timeSpan:Int,
             sample:Double):Unit = {
    val taxiData = sc.loadTaxi(inputDir, sample)
    val cells = taxiData.toCells(gridSize, timeSpan)
    val output = GetisOrd.calculate(cells)
    output.saveAsTextFile(outputDir)
  }
}
