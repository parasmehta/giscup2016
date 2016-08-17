package edu.fuberlin.hotspots

import java.io.{File, FileOutputStream}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.matching.Regex

/**
  * An implementation of the GetisOrd G* statistics that is tightliy tied to NYC taxi data from 2015.
  * (really, other years than 2015 are not supported)
  * It supports also only a very simple weight matrix, where all neighbouring cells are 1 and the others 0.
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
    val gridSize = args(2)
    val timeSpan = args(3)

    val conf = new SparkConf().setAppName("Fu-Berlin")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf.set("spark.driver.extraJavaOptions", "-XX:+UseCompressedOops")
    conf.set("spark.executor.extraJavaOptions", "-XX:+UseCompressedOops")
    conf.registerKryoClasses(Array(classOf[org.apache.spark.util.StatCounter],
      classOf[scala.math.Ordering$$anon$9], classOf[scala.math.Ordering$$anonfun$by$1],
      Class.forName("edu.fuberlin.hotspots.Submission$$anonfun$4"), Class.forName("scala.math.Ordering$Double$")
    ))
    val sc = new SparkContext(conf)
    submit(sc, inputDirectory, outputFile, gridSize, timeSpan)
  }

  def printHelp = {
    println("How to use it:")
    println("  spark-submit --class edu.fuberlin.hotspots.Submission --master someMasterUrl \\")
    println("  path/to/jar \\")
    println("  {hdfs,file,}://path/to/input_directory \\")
    println("  {hdfs,file,}://path/to/output_file \\")
    println("  {spatial gridsize in degrees (0.001 for example)}")
  }

  val dateRegex = new Regex("""^2015-(\d{2})-(\d{2}).*""")
  /**
    * compute the getis ord G* statistics with the given spark context
    * It outputs the 50 hottest zones.
    *
    * Designed to be used in the spark-shell as well by the main function, that is called by spark-submit
    * @param sc SparkContext, must not be stopped when calling this function
    * @param inputDir The directory or file to read the csv from.
    * @param outputFile File to store the result
    * @param gridSize Size of a grid in degrees. Minimum supported value: 0.001, can be either a String or a Double.
    * @param timeSpan number of days that span over a cell, can be either an Integer or a String.
    */
  def submit(sc:SparkContext, inputDir:String, outputFile:String, gridSize:Any, timeSpan:Any):Unit = {
    val gs = gridSize match {case s:String => s.toDouble case d:Double => d}
    val ts = timeSpan match {case s:String => s.toInt case i:Int => i}
    val taxiData = sc.textFile(inputDir)
    val xOrigin = (MIN_LONGITUDE / gs).toInt
    val yOrigin = (MIN_LATITUDE / gs).toInt
    val cells = taxiData.map{line =>
      try {
        val fields = line.split(",")
        //turned out we loose with the standard date parsing libraries about 10 secs, so we implemented
        //it ourself
        val dateRegex(m, d) = fields(2)
        val (month, day) = (m.toInt, d.toInt)
        val t = day + (month match {
          case 1 => 0 case 2 => 31 case 3 => 59 case 4 => 90 case 5 => 120 case 6 => 151 case 7 => 181 case 8 => 212
          case 9 => 243 case 10 => 273 case 11 => 304 case 12 => 334
        })
        val passengerCount = fields(3).toInt
        val longitude = fields(9).toDouble
        val latitude = fields(10).toDouble
        if(latitude >= MIN_LATITUDE && latitude <= MAX_LATITUDE &&
          longitude >= MIN_LONGITUDE && longitude <= MAX_LONGITUDE){
          Some((compose((longitude / gs).toInt - xOrigin, (latitude / gs).toInt - yOrigin, t / ts), passengerCount))
        } else {
          None
        }
      } catch{case e:Throwable => None}
    }.collect({case Some(t) => t}).reduceByKey(_ + _)
    val zvalues = GetisOrd.calculate(cells)
    val output = zvalues.top(50)(Ordering.by(_._2)).map(c=> (decompose(c._1), c._2, c._3))
      .map(c=> s"${c._1._1 + xOrigin}, ${c._1._2 + yOrigin}, ${c._1._3}, ${c._2}, ${c._3}")
    val stream = outputFile.slice(0,4).toLowerCase match {
      case "hdfs" => {
        val fs = FileSystem.get(sc.hadoopConfiguration)
        fs.create(new Path(outputFile))
      }
      case _ => new FileOutputStream(new File(outputFile))
    }
    output.foreach(line => stream.write((line + "\n").getBytes()))
    stream.flush()
    stream.close()
  }
}
