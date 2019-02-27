package glint.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.ExecutionContext

/**
  * Provides basic setup for Spark tests
  */
trait SparkTest extends ScalaFutures with BeforeAndAfterAll { this: Suite =>

  implicit val ec = ExecutionContext.Implicits.global

  implicit val defaultPatience = PatienceConfig(timeout = Span(60, Seconds), interval = Span(500, Millis))


  lazy val sc = new SparkContext(new SparkConf().setAppName(getClass.getSimpleName))

  override def afterAll(): Unit = {
    sc.stop()
  }
}
