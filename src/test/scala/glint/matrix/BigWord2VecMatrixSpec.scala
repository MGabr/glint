package glint.matrix

import breeze.linalg.Vector
import com.github.fommil.netlib.F2jBLAS
import glint.{HdfsTest, SystemTest, Word2VecArguments}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.{FlatSpec, Inspectors, Matchers}

/**
  * BigWord2VecMatrix test specification
  */
class BigWord2VecMatrixSpec extends FlatSpec with SystemTest with HdfsTest with Matchers with Inspectors {

  @transient
  private lazy val blas = new F2jBLAS

  implicit val tolerantFloatEq: Equality[Float] = TolerantNumerics.tolerantFloatEquality(0.0000001f)

  implicit val tolerantFloatArrayEq: Equality[Array[Float]] = new Equality[Array[Float]] {
    override def areEqual(a: Array[Float], b: Any): Boolean = b match {
      case br: Array[Float] =>
        a.length == br.length && a.zip(br).forall { case (ax, bx) => tolerantFloatEq.areEqual(ax, bx) }
      case brr: Array[_] => a.deep == brr.deep
      case _ => a == b
    }
  }

  implicit val tolerantFloatVectorEq: Equality[Vector[Float]] = new Equality[Vector[Float]] {
    override def areEqual(a: Vector[Float], b: Any): Boolean = b match {
      case br: Vector[Float] => tolerantFloatArrayEq.areEqual(a.toArray, br.toArray)
      case _ => a == b
    }
  }

  implicit val tolerantFloatVectorArrayEq: Equality[Array[Vector[Float]]] = new Equality[Array[Vector[Float]]]{
    override def areEqual(a: Array[Vector[Float]], b: Any): Boolean = b match {
      case br: Array[Vector[Float]] =>
        a.length == br.length && a.zip(br).forall { case (ax, bx) => tolerantFloatVectorEq.areEqual(ax, bx) }
      case _ => a == b
    }
  }

  val init = Array(
    Array(0.076989256f, 0.076959394f, 0.07704898f),
    Array(0.11048033f, -0.13317561f, -0.0688745f),
    Array(-0.08648787f, -0.02997307f, 0.13381587f),
    Array(0.03544839f, -0.030853411f, -0.16528131f)
  )

  "A BigWord2VecMatrix" should "initialize values randomly" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val args = Word2VecArguments(3, 1, 1, 0)
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(args, vocabCns, hadoopConfig)

        val values = whenReady(model.pull(
          Array(0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3),
          Array(0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2))) {
          identity
        }

        values should equal(init.flatten)
      }
    }
  }

  it should "not adjust input weights on first gradient update" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val args = Word2VecArguments(3, 1, 2, 3)
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(args, vocabCns, hadoopConfig)
        val seed = 1

        // random negative words will be 2, 3, 3

        var result = whenReady(model.adjust(
          Array(1, 0),
          Array(Array(0), Array(1)),
          Array(0.11f, 0.12f),
          Array(0.21f, 0.22f, 0.23f, 0.24f, 0.25f, 0.26f),
          seed)) {
          identity
        }
        assert(result)
        val values = whenReady(model.pull(
          Array(0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3),
          Array(0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2))) {
          identity
        }

        values should equal(init.flatten)
      }
    }
  }

  it should "adjust input weights on following gradient updates for same input and output" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val args = Word2VecArguments(3, 1, 1, 2)
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(args, vocabCns, hadoopConfig)
        val seed = 1

        // random negative words will be 2, 3, 3

        var result = whenReady(model.adjust(Array(1), Array(Array(1)), Array(0.11f), Array(0.21f, 0.22f), seed)) {
          identity
        }
        assert(result)
        result = whenReady(model.adjust(Array(1), Array(Array(1)), Array(0.111f), Array(0.211f, 0.221f), seed)) {
          identity
        }
        assert(result)
        val values = whenReady(model.pull(
          Array(0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3),
          Array(0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2))) {
          identity
        }

        values should equal(Array(
          init(0)(0),
          init(0)(1),
          init(0)(2),
          init(1)(0) + 0.111f * (0.11f * init(1)(0)) + 0.211f * (0.21f * init(1)(0)) + 0.221f * (0.22f * init(1)(0)),
          init(1)(1) + 0.111f * (0.11f * init(1)(1)) + 0.211f * (0.21f * init(1)(1)) + 0.221f * (0.22f * init(1)(1)),
          init(1)(2) + 0.111f * (0.11f * init(1)(2)) + 0.211f * (0.21f * init(1)(2)) + 0.221f * (0.22f * init(1)(2)),
          init(2)(0),
          init(2)(1),
          init(2)(2),
          init(3)(0),
          init(3)(1),
          init(3)(2)))
      }
    }
  }

  it should "adjust input weights on following gradient updates for different input and output" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val args = Word2VecArguments(3, 1, 2, 0)
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(args, vocabCns, hadoopConfig)
        val seed = 1

        var result = whenReady(model.adjust(Array(1, 0), Array(Array(0), Array(1)), Array(0.11f, 0.12f), Array(), seed)) {
          identity
        }
        assert(result)
        result = whenReady(model.adjust(Array(1, 0), Array(Array(0), Array(1)), Array(0.111f, 0.121f), Array(), seed)) {
          identity
        }
        assert(result)

        val values = whenReady(model.pull(
          Array(0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3),
          Array(0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2))) {
          identity
        }

        values should equal(Array(
          init(0)(0) + 0.121f * (0.12f * init(0)(0)),
          init(0)(1) + 0.121f * (0.12f * init(0)(1)),
          init(0)(2) + 0.121f * (0.12f * init(0)(2)),
          init(1)(0) + 0.111f * (0.11f * init(1)(0)),
          init(1)(1) + 0.111f * (0.11f * init(1)(1)),
          init(1)(2) + 0.111f * (0.11f * init(1)(2)),
          init(2)(0),
          init(2)(1),
          init(2)(2),
          init(3)(0),
          init(3)(1),
          init(3)(2)))
      }
    }
  }

  it should "adjust input weights on following gradient updates with negative examples" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val args = Word2VecArguments(3, 1, 1, 2)
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(args, vocabCns, hadoopConfig)
        val seed = 1

        // random negative words will be 2, 3, 3

        var result = whenReady(model.adjust(Array(1), Array(Array(0)), Array(0.11f), Array(0.21f, 0.22f), seed)) {
          identity
        }
        assert(result)
        result = whenReady(model.adjust(Array(1), Array(Array(0)), Array(0.111f), Array(0.211f, 0.221f), seed)) {
          identity
        }
        assert(result)
        val values = whenReady(model.pull(
          Array(0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3),
          Array(0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2))) {
          identity
        }

        values should equal(Array(
          init(0)(0),
          init(0)(1),
          init(0)(2),
          init(1)(0) + 0.111f * (0.11f * init(1)(0)) + 0.211f * (0.21f * init(1)(0)) + 0.221f * (0.22f * init(1)(0)),
          init(1)(1) + 0.111f * (0.11f * init(1)(1)) + 0.211f * (0.21f * init(1)(1)) + 0.221f * (0.22f * init(1)(1)),
          init(1)(2) + 0.111f * (0.11f * init(1)(2)) + 0.211f * (0.21f * init(1)(2)) + 0.221f * (0.22f * init(1)(2)),
          init(2)(0),
          init(2)(1),
          init(2)(2),
          init(3)(0),
          init(3)(1),
          init(3)(2)))
      }
    }
  }

  it should "compute dot products as zero if no adjust has been made" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val args = Word2VecArguments(3, 1, 2, 0)
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(args, vocabCns, hadoopConfig)
        val seed = 1

        val value = whenReady(model.dotprod(Array(0), Array(Array(1, 0)), seed)) {
          identity
        }

        value._1 should equal(Array(0.0f, 0.0f))
        value._2 should equal(Array())
      }
    }
  }

  it should "compute dot products of input and output weights" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val args = Word2VecArguments(3, 1, 2, 0)
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(args, vocabCns, hadoopConfig)
        val seed = 1

        val result = whenReady(model.adjust(Array(1), Array(Array(1)), Array(0.11f), Array(), seed)) {
          identity
        }
        assert(result)
        val value = whenReady(model.dotprod(Array(1), Array(Array(1, 0)), seed)) {
          identity
        }

        value._1 should equal(Array(
          init(1)(0) * (0.11f * init(1)(0)) + init(1)(1) * (0.11f * init(1)(1)) + init(1)(2) * (0.11f * init(1)(2)),
          0f
        ))
        value._2 should equal(Array())
      }
    }
  }

  it should "compute dot products of input and output weights with negative examples" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val args = Word2VecArguments(3, 1, 2, 2)
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(args, vocabCns, hadoopConfig)
        val seed = 1

        // random negative words will be 2, 3, 3, 2

        val result = whenReady(model.adjust(Array(1), Array(Array(1)), Array(0.11f), Array(0.21f, 0.22f), seed)) {
          identity
        }
        assert(result)
        val value = whenReady(model.dotprod(Array(1), Array(Array(1, 0)), seed)) {
          identity
        }

        value._1 should equal(Array(
          init(1)(0) * (0.11f * init(1)(0)) + init(1)(1) * (0.11f * init(1)(1)) + init(1)(2) * (0.11f * init(1)(2)),
          0f
        ))
        value._2 should equal(Array(
          init(1)(0) * (0.21f * init(1)(0)) + init(1)(1) * (0.21f * init(1)(1)) + init(1)(2) * (0.21f * init(1)(2)),
          init(1)(0) * (0.22f * init(1)(0)) + init(1)(1) * (0.22f * init(1)(1)) + init(1)(2) * (0.22f * init(1)(2)),
          init(1)(0) * (0.22f * init(1)(0)) + init(1)(1) * (0.22f * init(1)(1)) + init(1)(2) * (0.22f * init(1)(2)),
          init(1)(0) * (0.21f * init(1)(0)) + init(1)(1) * (0.21f * init(1)(1)) + init(1)(2) * (0.21f * init(1)(2))
        ))
      }
    }
  }

  it should "compute euclidean norms" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val args = Word2VecArguments(3, 1, 1, 0)
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(args, vocabCns, hadoopConfig)

        val values = whenReady(model.norms()) {
          identity
        }

        values should equal(Array[Float](
          blas.snrm2(3, init(0), 1),
          blas.snrm2(3, init(1), 1),
          blas.snrm2(3, init(2), 1),
          blas.snrm2(3, init(3), 1)))
      }
    }
  }

  it should "compute multiplication of matrix with vector" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val args = Word2VecArguments(3, 1, 1, 0)
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(args, vocabCns, hadoopConfig)
        val vector = Array(0.1f, 0.2f, 0.3f)

        val values = whenReady(model.multiply(vector)) {
          identity
        }

        val matrix = init.flatten
        val rows = 4
        val cols = 3
        val resultVector = new Array[Float](rows)
        val alpha: Float = 1
        val beta: Float = 0
        blas.sgemv("T", cols, rows, alpha, matrix, cols, vector, 1, beta, resultVector, 1)

        values should equal(resultVector)
      }
    }
  }

  it should "pull average of rows" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val args = Word2VecArguments(3, 1, 1, 0)
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(args, vocabCns, hadoopConfig)

        val values = whenReady(model.pullAverage(Array(Array(0, 2, 3), Array(), Array(0, 2), Array(0)))) {
          identity
        }

        values should equal(Array(
          Vector((init(0), init(2), init(3)).zipped.map((x, y, z) => (x + y + z) / 3)),
          Vector(0f, 0f, 0f),
          Vector((init(0), init(2)).zipped.map((x, y) => (x + y) / 2)),
          Vector(init(0))
        ))
      }
    }
  }

  it should "save data to file" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val args = Word2VecArguments(3, 1, 1, 0)
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(args, vocabCns, hadoopConfig)
        val seed = 1

        var result = whenReady(model.push(Array(0L, 0L, 0L), Array(0, 1, 2), Array(0.1f, 0.3f, 0.5f))) {
          identity
        }
        assert(result)
        result = whenReady(model.adjust(Array(2), Array(Array(2)), Array(0.22f), Array(), seed)) {
          identity
        }
        assert(result)
        result = whenReady(model.save("testdata", hadoopConfig)) {
          identity
        }
        assert(result)

        val fs = FileSystem.get(hadoopConfig)
        val paths = Seq(
          "testdata",
          "testdata/glint",
          "testdata/glint/metadata",
          "testdata/glint/data/u/0",
          "testdata/glint/data/u/1",
          "testdata/glint/data/u/2",
          "testdata/glint/data/v/0",
          "testdata/glint/data/v/1",
          "testdata/glint/data/v/2"
        )
        forAll (paths) {path => fs.exists(new Path(path)) shouldBe true }
      }
    }
  }

  it should "load data from file" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        if (!FileSystem.get(hadoopConfig).exists(new Path("testdata"))) {
          pending
        }

        val loadedModel = client.loadWord2vecMatrix("testdata", hadoopConfig)

        val values = whenReady(loadedModel.pull(Array(0, 0, 0, 3, 3), Array(0, 1, 2, 0, 1))) {
          identity
        }

        values should equal(Array(init(0)(0) + 0.1f, init(0)(1) + 0.3f, init(0)(2) + 0.5f, init(3)(0), init(3)(1)))
      }
    }
  }

  it should "retrain data loaded from file" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        if (!FileSystem.get(hadoopConfig).exists(new Path("testdata"))) {
          pending
        }

        val loadedModel = client.loadWord2vecMatrix("testdata", hadoopConfig, trainable = true)
        val seed = 1

        val result = whenReady(loadedModel.adjust(Array(1), Array(Array(1)), Array(0.11f), Array(), seed)) {
          identity
        }
        assert(result)
        val value = whenReady(loadedModel.dotprod(Array(1), Array(Array(1, 2)), seed)) {
          identity
        }

        value._1 should equal(Array(
          init(1)(0) * (0.11f * init(1)(0)) + init(1)(1) * (0.11f * init(1)(1)) + init(1)(2) * (0.11f * init(1)(2)),
          init(1)(0) * (0.22f * init(2)(0)) + init(1)(1) * (0.22f * init(2)(1)) + init(1)(2) * (0.22f * init(2)(2))
        ))
        value._2 should equal(Array())
      }
    }
  }

  it should "save untrainable data to file" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val args = Word2VecArguments(3, 1, 1, 0)
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(args, vocabCns, hadoopConfig)

        var result = whenReady(model.push(Array(0L, 0L, 0L), Array(0, 1, 2), Array(0.1f, 0.3f, 0.5f))) {
          identity
        }
        assert(result)
        result = whenReady(model.save("testdata-untrainable", hadoopConfig, trainable = false)) {
          identity
        }
        assert(result)

        val fs = FileSystem.get(hadoopConfig)
        val paths = Seq(
          "testdata-untrainable",
          "testdata-untrainable/glint",
          "testdata-untrainable/glint/metadata",
          "testdata-untrainable/glint/data/u/0",
          "testdata-untrainable/glint/data/u/1",
          "testdata-untrainable/glint/data/u/2"
        )
        val pathsNot = Seq(
          "testdata-untrainable/glint/data/v/0",
          "testdata-untrainable/glint/data/v/1",
          "testdata-untrainable/glint/data/v/2"
        )
        forAll (paths) {path => fs.exists(new Path(path)) shouldBe true }
        forAll (pathsNot) {path => fs.exists(new Path(path)) shouldBe false }
      }
    }
  }

  it should "load untrainable data from file" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        if (!FileSystem.get(hadoopConfig).exists(new Path("testdata-untrainable"))) {
          pending
        }

        val loadedModel = client.loadWord2vecMatrix("testdata-untrainable", hadoopConfig)

        val values = whenReady(loadedModel.pull(Array(0, 0, 0, 3, 3), Array(0, 1, 2, 0, 1))) {
          identity
        }

        values should equal(Array(init(0)(0) + 0.1f, init(0)(1) + 0.3f, init(0)(2) + 0.5f, init(3)(0), init(3)(1)))
      }
    }
  }

  it should "fail when attempting to retrain untrainable data" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        if (!FileSystem.get(hadoopConfig).exists(new Path("testdata-untrainable"))) {
          pending
        }

        val loadedModel = client.loadWord2vecMatrix("testdata-untrainable", hadoopConfig)

        an[IllegalArgumentException] shouldBe thrownBy(
          loadedModel.adjust(Array(1), Array(Array(1)), Array(0.11f), Array(), 1))
        an[IllegalArgumentException] shouldBe thrownBy(
          loadedModel.dotprod(Array(1), Array(Array(1, 0)), 1))
      }
    }
  }
}
