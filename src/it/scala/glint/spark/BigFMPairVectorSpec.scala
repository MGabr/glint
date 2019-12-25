package glint.spark

import breeze.linalg._
import glint.{Client, FMPairArguments}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{FlatSpec, Inspectors, Matchers}

import scala.math.{exp, sqrt}

/**
 * BigFMPairVector integration test specification
 * Similar to BigFMPairVector system test specification
 */
class BigFMPairVectorSpec extends FlatSpec with SparkTest with Matchers with Inspectors with TolerantFloat {

  val init = Map(
    0 -> 0.046193548f,
    5 -> -0.038189888f,
    9000 -> -0.031283297f,
    50000 -> 0.046175636f,
    90000 -> -0.05671072f,
    90100 -> -0.09171947f
  )

  "A BigFMPairVector" should "initialize values randomly" in {
    val client = Client.runOnSpark(sc)()
    try {
      val args = FMPairArguments(batchSize=3)
      val numFeatures = 100000
      val model= client.fmpairVector(args, numFeatures)

      val values = whenReady(model.pull(Array(0, 5, 9000, 50000, 90000, 90100))) {
        identity
      }

      values should equal(Array(0, 5, 9000, 50000, 90000, 90100).map(init))
    } finally {
      client.terminateOnSpark(sc)
    }
  }

  it should "compute sums" in {
    val client = Client.runOnSpark(sc)()
    try {
      val args = FMPairArguments(batchSize=3)
      val numFeatures = 100000
      val model = client.fmpairVector(args, numFeatures)

      val indices = Array(Array(0, 50000, 90100), Array(0, 90000), Array(5, 9000, 50000, 90100))
      val weights = Array(Array(1.0f, 1.0f, 0.3f), Array(1.0f, 1.0f), Array(1.0f, 0.25f, 1.0f, 0.3f))

      val (s, cacheKeys) = whenReady(model.pullSum(indices, weights)) {
        identity
      }

      s should equal(Array(
        init(0) + init(50000) + 0.3f * init(90100),
        init(0) + init(90000),
        init(5) + 0.25f * init(9000) + init(50000) + 0.3f * init(90100)
      ))
      cacheKeys should equal(Array(1, 1))
    } finally {
      client.terminateOnSpark(sc)
    }
  }

  it should "adjust weights" in {
    val client = Client.runOnSpark(sc)()
    try {
      val args = FMPairArguments(batchSize=3)
      val numFeatures = 100000
      val model = client.fmpairVector(args, numFeatures)

      val indices = Array(Array(0, 50000, 90100), Array(0, 90000), Array(5, 9000, 50000, 90100))
      val weights = Array(Array(1.0f, 1.0f, 0.3f), Array(1.0f, 1.0f), Array(1.0f, 0.25f, 1.0f, 0.3f))

      val (s, cacheKeys) = whenReady(model.pullSum(indices, weights)) {
        identity
      }

      val g = s.map(e => exp(-e)).map(e => (e / (1 + e)).toFloat)  // general BPR gradient

      val result = whenReady(model.pushSum(g, cacheKeys)) {
        identity
      }
      assert(result)

      val values = whenReady(model.pull(Array(0, 5, 9000, 50000, 90000, 90100))) { identity }

      val ada = sqrt(0.1 + 1e-07).toFloat  // initial Adagrad learning rate

      values should equal(Array(
        init(0) + args.lr / ada * (g(0) + g(1) - args.linearReg * init(0)),
        init(5) + args.lr / ada * (g(2) - args.linearReg * init(5)),
        init(9000) + args.lr / ada * (g(2) * 0.25f - args.linearReg * init(9000)),
        init(50000) + args.lr / ada * (g(0) + g(2) - args.linearReg * init(50000)),
        init(90000) + args.lr / ada * (g(1) - args.linearReg * init(90000)),
        init(90100) + args.lr / ada * (g(0) * 0.3f + g(2) * 0.3f - args.linearReg * init(90100))
      ))
    } finally {
      client.terminateOnSpark(sc)
    }
  }

  it should "save data to file" in {
    val client = Client.runOnSpark(sc)()
    try {
      val args = FMPairArguments(batchSize=3)
      val numFeatures = 100000
      val avgActiveFeatures = 3
      val model = client.fmpairVector(args, numFeatures, avgActiveFeatures)

      var result = whenReady(model.push(Array(5, 90100), Array(0.1f, 0.2f))) { identity }
      assert(result)
      result = whenReady(model.save("testdata", sc.hadoopConfiguration)) { identity }
      assert(result)

      val fs = FileSystem.get(sc.hadoopConfiguration)
      val paths = Seq(
        "testdata",
        "testdata/glint",
        "testdata/glint/metadata",
        "testdata/glint/data/v/0",
        "testdata/glint/data/v/1",
        "testdata/glint/data/b/0",
        "testdata/glint/data/b/1"
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
      val loadedModel = client.loadFMPairVector("testdata", sc.hadoopConfiguration)

      val values = whenReady(loadedModel.pull(Array(5, 90100))) { identity }
      values should equal(Array(init(5) + 0.1f, init(90100) + 0.2f))
    } finally {
      client.terminateOnSpark(sc)
    }
  }
}