package edu.fuberlin.hotspots

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.math.BigDecimal

import org.apache.spark.{SparkConf, SparkContext}
import SparkHelpers._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD

import scala.util.matching.Regex

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

    val gridSize = args(2)
    val timeSpan = args(3)

    val sample = args.lift(4).getOrElse("1").toDouble
    val conf = new SparkConf().setAppName("Fu-Berlin")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf.registerKryoClasses(Array(classOf[Trip], classOf[SuperCell], classOf[SuperCellFactory],
      classOf[Option[Trip]], classOf[(Cellid, Int)], classOf[(Cellid, Double)],
      classOf[Array[Double]], classOf[org.apache.spark.util.StatCounter],
      classOf[scala.reflect.ClassTag$$anon$1], classOf[java.lang.Class[_]],
      classOf[scala.collection.mutable.WrappedArray$ofRef], classOf[Array[String]],
      classOf[scala.math.Ordering$$anon$9], classOf[scala.math.Ordering$$anonfun$by$1],
      Class.forName("edu.fuberlin.hotspots.Submission$$anonfun$5"), Class.forName("scala.math.Ordering$Double$")
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
  def submit(sc:SparkContext,
             inputDir:String,
             outputFile:String,
             gridSize:Any,
             timeSpan:Any):Unit = {
    val gs = gridSize match {case s:String => s.toDouble case d:Double => d}
    val ts = timeSpan match {case s:String => s.toInt case i:Int => i}
    val taxiData = sc.textFile(inputDir)
    val cells = taxiData.map{line =>
      try {
        val fields = line.split(",")
        val dateRegex(m, d) = fields(2)
        val (month, day) = (m.toInt, d.toInt)
        val t = day + (month match {
          case 1 => 0
          case 2 => 31
          case 3 => 59
          case 4 => 90
          case 5 => 120
          case 6 => 151
          case 7 => 181
          case 8 => 212
          case 9 => 243
          case 10 => 273
          case 11 => 304
          case 12 => 334
        })
        val passengerCount = fields(3).toInt
        val longitude = fields(9).toDouble
        val latitude = fields(10).toDouble
        if(latitude >= 40.5 && latitude <= 40.9 && longitude >= -74.25 && longitude <= -73.4){
          Some((compose((longitude / gs).toInt, (latitude / gs).toInt, t / ts), passengerCount))
        } else {
          None
        }
      } catch{case e:Throwable => None}
    }.collect({case Some(t) => t}).reduceByKey(_ + _).cache
    val zvalues = GetisOrd.calculate(cells)
    val output = zvalues.top(50)(Ordering.by(_._2)).map(c=> s"${c._1._1}, ${c._1._2}, ${c._1._3}, ${c._2}, ${c._3}")
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
