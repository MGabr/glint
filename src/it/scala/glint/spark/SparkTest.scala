package glint.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.Tag


object SparkTest extends Tag("SparkTest")

/**
  * Provides basic functions for Spark tests
  */
trait SparkTest {

  /**
    * Fixture that starts a Spark context before running test code and stops it afterwards
    *
    * @param testCode The test code to run
    */
  def withContext(testCode: SparkContext => Any): Unit = {
    val conf = new SparkConf().setAppName("Glint SparkTest")
    val sc = new SparkContext(conf)
    try {
      testCode(sc)
    } finally {
      sc.stop()
    }
  }
}
