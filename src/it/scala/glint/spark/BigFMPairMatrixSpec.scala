package glint.spark

import breeze.linalg._
import glint.{Client, FMPairArguments}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{FlatSpec, Inspectors, Matchers}

import scala.math.{exp, sqrt}

/**
 * BigFMPairMatrix integration test specification
 * Similar to BigFMPairMatrix system test specification
 */
class BigFMPairMatrixSpec extends FlatSpec with SparkTest with Matchers with Inspectors with TolerantFloat {

  val init = Map(
    0 -> DenseVector(0.046193548f, 0.066288196f, -0.051892724f, 0.046175636f, -0.07990537f, -0.017983839f, -0.018512048f),
    5 -> DenseVector(0.0965639f, 0.0758365f, -0.09538364f, -0.020565137f, 0.08279257f, -0.030496396f, -0.06813209f),
    9000 -> DenseVector(0.033150934f, 0.024559006f, -0.0085680485f, 0.030069418f, -0.027467623f, -0.019553326f, -0.081428334f),
    50000 -> DenseVector(0.017069533f, -0.02722641f, 0.03263382f, 0.098965295f, 0.010900959f, 0.020128705f, 0.081559844f),
    90000 -> DenseVector(-0.09282738f, 0.07148876f, 0.013867363f, 0.026183598f, -0.007840611f, 0.020899728f, -0.06487197f),
    90100 -> DenseVector(0.08719083f, -0.026967607f, 0.0764989f, 0.03886003f, -0.006404504f, 0.07445846f, 0.0011448637f)
  )

  val args = FMPairArguments(k=7, batchSize=3)
  val featureProbs = Array.fill[Float](100000)(0.1f)
  val c = Array.fill[Float](100000)(0.90333333333f)
  for (i <- Array(0, 50000, 90100)) {
    featureProbs(i) = 0.66f
    c(i) = 0.4852f
  }

  "A BigFMPairMatrix" should "initialize values randomly" in {
    val client = Client.runOnSpark(sc)()
    try {
      val matrix = client.fmpairMatrix(args, featureProbs, sc.hadoopConfiguration, 1)

      val values = whenReady(matrix.pull(Array(0, 5, 9000, 50000, 90000, 90100))) {
        identity
      }

      values should equal(Array(0, 5, 9000, 50000, 90000, 90100).map(init))
    } finally {
      client.terminateOnSpark(sc)
    }
  }

  it should "compute dot products" in {
    val client = Client.runOnSpark(sc)()
    try {
      val matrix = client.fmpairMatrix(args, featureProbs, sc.hadoopConfiguration, 1)

      val iUser = Array(Array(0), Array(0), Array(5, 9000))
      val wUser = Array(Array(1.0f), Array(1.0f), Array(1.0f, 0.25f))
      val iItem = Array(Array(50000, 90100), Array(90000), Array(50000, 90100))
      val wItem = Array(Array(1.0f, 0.3f), Array(1.0f), Array(1.0f, 0.3f))

      val (f, cacheKeys) = whenReady(matrix.dotprod(iUser, wUser, iItem, wItem)) {
        identity
      }

      // breeze "x dot y" instead of "sum(x *:* y)" seems to have floating point precision problems and returns 0.0
      f should equal(Array(
        sum(init(0) *:* (init(50000) + 0.3f * init(90100))),
        sum(init(0) *:* init(90000)),
        sum((init(5) + 0.25f * init(9000)) *:* (init(50000) + 0.3f * init(90100)))
      ))
      cacheKeys should equal(Array(0, 0))
    } finally {
      client.terminateOnSpark(sc)
    }
  }

  it should "adjust weights" in {
    val client = Client.runOnSpark(sc)()
    try {
      val matrix = client.fmpairMatrix(args, featureProbs, sc.hadoopConfiguration, 1)

      val iUser = Array(Array(0), Array(0), Array(5, 9000))
      val wUser = Array(Array(1.0f), Array(1.0f), Array(1.0f, 0.25f))
      val iItem = Array(Array(50000, 90100), Array(90000), Array(50000, 90100))
      val wItem = Array(Array(1.0f, 0.3f), Array(1.0f), Array(1.0f, 0.3f))

      val (f, cacheKeys) = whenReady(matrix.dotprod(iUser, wUser, iItem, wItem)) {
        identity
      }

      val g = f.map(e => exp(-e)).map(e => (e / (1 + e)).toFloat)  // general BPR gradient

      val result = whenReady(matrix.adjust(g, cacheKeys)) {
        identity
      }
      assert(result)

      val values = whenReady(matrix.pull(Array(0, 5, 9000, 50000, 90000, 90100))) { identity }

      values should equal(Array(
        init(0) + args.lr * c(0) * (g(0) * (init(50000) + 0.3f * init(90100)) + g(1) * init(90000) - 2 * args.factorsReg * init(0)),
        init(5) + args.lr * c(5) * (g(2) * (init(50000) + 0.3f * init(90100)) - args.factorsReg * init(5)),
        init(9000) + args.lr * c(9000) * (0.25f * g(2) * (init(50000) + 0.3f * init(90100)) - args.factorsReg * init(9000)),
        init(50000) + args.lr * c(50000) * (g(0) * init(0) + g(2) * (init(5) + 0.25f * init(9000)) - 2 * args.factorsReg * init(50000)),
        init(90000) + args.lr * c(90000) * (g(1) * init(0) - args.factorsReg * init(90000)),
        init(90100) + args.lr * c(90100) * (0.3f * (g(0) * init(0) + g(2) * (init(5) + 0.25f * init(9000))) - 2 * args.factorsReg * init(90100))
      ))
    } finally {
      client.terminateOnSpark(sc)
    }
  }

  it should "save data to file" in {
    val client = Client.runOnSpark(sc)()
    try {
      val matrix = client.fmpairMatrix(args, featureProbs, sc.hadoopConfiguration, 1)

      var result = whenReady(matrix.push(
        Array(5, 5, 5, 5, 5),
        Array(0, 1, 2, 3, 4),
        Array(0.1f, 0.2f, 0.3f, 0.4f, 0.5f)
      )) {
        identity
      }
      assert(result)
      result = whenReady(matrix.save("testdata", sc.hadoopConfiguration)) {
        identity
      }
      assert(result)

      val fs = FileSystem.get(sc.hadoopConfiguration)
      val paths = Seq(
        "testdata",
        "testdata/glint",
        "testdata/glint/metadata",
        "testdata/glint/data/v/0",
        "testdata/glint/data/v/1"
      )
      forAll (paths) {path => fs.exists(new Path(path)) shouldBe true }
    } finally {
      client.terminateOnSpark(sc)
    }
  }

  it should "load data from file" in {
    if (!FileSystem.get(sc.hadoopConfiguration).exists(new Path("testdata"))) {
      pending
    }

    val client = Client.runOnSpark(sc)()
    try {
      val loadedMatrix = client.loadFMPairMatrix("testdata", sc.hadoopConfiguration, 1)

      val values = whenReady(loadedMatrix.pull(Array(5))) { identity }
      values(0) should equal(init(5) + DenseVector(0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.0f, 0.0f))
    } finally {
      client.terminateOnSpark(sc)
    }
  }
}