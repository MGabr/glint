package glint.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{TestData, fixture}
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.ExecutionContext

/**
  * Provides basic functions for Spark tests
  */
trait SparkTest extends ScalaFutures { this: fixture.TestDataFixture =>

  implicit val ec = ExecutionContext.Implicits.global

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(60, Seconds), interval = Span(500, Millis))

  /**
    * Fixture that starts a Spark context before running test code and stops it afterwards
    *
    * @param testCode The test code to run
    */
  def withContext(testCode: SparkContext => Any): TestData => Any = { td =>
    val conf = new SparkConf().setAppName(td.name)
    val sc = new SparkContext(conf)
    try {
      testCode(sc)
    } finally {
      sc.stop()
    }
  }
}
