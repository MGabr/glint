package glint.spark

import glint.Client
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.{Inspectors, Matchers, fixture}

/**
  * BigWord2Vec matrix integration test specification
  * Similar to BigWord2Vec matrix system test specification
  */
class BigWord2VecMatrixSpec extends fixture.FlatSpec with fixture.TestDataFixture with SparkTest
  with Matchers with Inspectors {

  implicit val tolerantFloatEq: Equality[Float] = TolerantNumerics.tolerantFloatEquality(0.0000001f)

  implicit val tolerantFloatArrayEq: Equality[Array[Float]] = new Equality[Array[Float]] {
    override def areEqual(a: Array[Float], b: Any): Boolean = b match {
      case br: Array[Float] =>
        a.length == br.length && a.zip(br).forall { case (ax, bx) => tolerantFloatEq.areEqual(ax, bx) }
      case brr: Array[_] => a.deep == brr.deep
      case _ => a == b
    }
  }

  val init = Array.ofDim[Float](1000, 0)
  init(0) = Array(0.0023096777f, 0.0033144099f, -0.002594636f)
  init(709) = Array(0.0010975379f, 0.004738921f, -0.0046323584f)
  init(857) = Array(-0.0030795962f, -6.597167E-4f, -5.274969E-4f)
  init(999) = Array(0.003140425f, -0.003970925f, -0.0024834543f)


  "A BigWord2VecMatrix" should "initialize values randomly" in withContext { sc =>
    val vocabCns = (1 to 1000).toArray
    val bcVocabCns = sc.broadcast(vocabCns)
    val (client, matrix) = Client.runWithWord2VecMatrixOnSpark(sc)(bcVocabCns, 100, 10, 1000000)

    try {
      val values = whenReady(matrix.pull(
        Array(0, 0, 0, 709, 709, 709, 857, 857, 857, 999, 999, 999),
        Array(0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2))) {
        identity
      }

      values should equal(init(0) ++ init(709) ++ init(857) ++ init(999))
    } finally {
      client.terminateOnSpark(sc)
    }
  }

  it should "adjust input weights" in withContext { sc =>
    val vocabCns = (1 to 1000).toArray
    val bcVocabCns = sc.broadcast(vocabCns)
    val (client, matrix) = Client.runWithWord2VecMatrixOnSpark(sc)(bcVocabCns, 100, 2, 1000000)

    try {
      val seed = 1

      // random negative words will be 709, 857

      var result = whenReady(matrix.adjust(
        Array(0),
        Array(Array(999)),
        Array(0.11f),
        Array(0.21f, 0.22f),
        seed)) {
        identity
      }
      assert(result)

      result = whenReady(matrix.adjust(
        Array(0),
        Array(Array(999)),
        Array(0.111f),
        Array(0.211f, 0.221f),
        seed)) {
        identity
      }
      assert(result)

      val values = whenReady(matrix.pull(
        Array(0, 0, 0, 999, 999, 999),
        Array(0, 1, 2, 0, 1, 2))) {
        identity
      }

      values should equal(Array(
        init(0)(0) + 0.111f * (0.11f * init(0)(0)) + 0.211f * (0.21f * init(0)(0)) + 0.221f * (0.22f * init(0)(0)),
        init(0)(1) + 0.111f * (0.11f * init(0)(1)) + 0.211f * (0.21f * init(0)(1)) + 0.221f * (0.22f * init(0)(1)),
        init(0)(2) + 0.111f * (0.11f * init(0)(2)) + 0.211f * (0.21f * init(0)(2)) + 0.221f * (0.22f * init(0)(2)),
        init(999)(0),
        init(999)(1),
        init(999)(2)))
    } finally {
      client.terminateOnSpark(sc)
    }
  }

  it should "compute dot products" in withContext { sc =>
    val vocabCns = (1 to 1000).toArray
    val bcVocabCns = sc.broadcast(vocabCns)
    val (client, matrix) = Client.runWithWord2VecMatrixOnSpark(sc)(bcVocabCns, 100, 2, 1000000)

    try {
      val initValues = whenReady(matrix.pull(Array.fill(100)(0), (0L until 100L).toArray)) {
        identity
      }

      val seed = 1

      val result = whenReady(matrix.adjust(
        Array(0),
        Array(Array(0)),
        Array(0.11f),
        Array(0.21f, 0.22f),
        seed)) {
        identity
      }
      assert(result)

      // random negative words will be 709, 857

      val value = whenReady(matrix.dotprod(
        Array(0),
        Array(Array(0)),
        seed)) {
        identity
      }

      value._1 should equal(Array(initValues.map(v => v * (0.11f * v)).sum))
      value._2 should equal(Array(initValues.map(v => v * (0.21f * v)).sum, initValues.map(v => v * (0.22f * v)).sum))
    } finally {
      client.terminateOnSpark(sc)
    }
  }

  it should "save data to file" in withContext { sc =>
    val vocabCns = (1 to 1000).toArray
    val bcVocabCns = sc.broadcast(vocabCns)
    val (client, matrix) = Client.runWithWord2VecMatrixOnSpark(sc)(bcVocabCns, 100, 2, 1000000)

    try {

      var result = whenReady(matrix.push(Array(0L, 0L, 0L), Array(0, 1, 2), Array(0.1f, 0.3f, 0.5f))) {
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
        "testdata/glint/data/u/0",
        "testdata/glint/data/u/1",
        "testdata/glint/data/v/0",
        "testdata/glint/data/v/1"
      )
      forAll (paths) {path => fs.exists(new Path(path)) shouldBe true }
    } finally {
      client.terminateOnSpark(sc)
    }
  }

  it should "load data from file" in withContext { sc =>
    if (!FileSystem.get(sc.hadoopConfiguration).exists(new Path("testdata"))) {
      pending
    }

    val (client, loadedMatrix) = Client.runWithLoadedWord2VecMatrixOnSpark(sc, "testdata")

    try {
      val values = whenReady(loadedMatrix.pull(Array(0, 0, 0, 999, 999), Array(0, 1, 2, 0, 1))) {
        identity
      }

      values should equal(Array(init(0)(0) + 0.1f, init(0)(1) + 0.3f, init(0)(2) + 0.5f, init(999)(0), init(999)(1)))
    } finally {
      client.terminateOnSpark(sc)
    }
  }
}
