package edu.fuberlin.hotspots
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Outcome}
import org.scalatest.fixture

/**
  * Created by Christian Windolf on 06.07.16.
  */
class SparkSpec extends fixture.FlatSpec {
  case class FixtureParam(context:SparkContext)
  override def withFixture(test:OneArgTest): Outcome ={
    val conf = new SparkConf().setAppName("Test").setMaster("local")
    val sc = new SparkContext(conf)
    try{withFixture(test.toNoArgTest(FixtureParam(sc)))}
    finally{sc.stop()}
  }
}
