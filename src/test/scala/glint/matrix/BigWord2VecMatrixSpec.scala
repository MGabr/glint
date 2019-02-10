package glint.matrix

import com.github.fommil.netlib.F2jBLAS
import glint.SystemTest
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.{FlatSpec, Matchers}

/**
  * BigWord2VecMatrix test specification
  */
class BigWord2VecMatrixSpec extends FlatSpec with SystemTest with Matchers {

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

  val init = Array(
    Array(0.076989256f, 0.076959394f, 0.07704898f),
    Array(0.11048033f, -0.13317561f, -0.0688745f),
    Array(-0.08648787f, -0.02997307f, 0.13381587f),
    Array(0.03544839f, -0.030853411f, -0.16528131f)
  )

  "A BigWord2VecMatrix" should "initialize values randomly" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(vocabCns, 3, 0)

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
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(vocabCns, 3, 3)
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
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(vocabCns, 3, 2)
        val seed = 1

        // random negative words will be 2, 3, 3

        var result = whenReady(model.adjust(
          Array(1),
          Array(Array(1)),
          Array(0.11f),
          Array(0.21f, 0.22f),
          seed)) {
          identity
        }
        assert(result)

        result = whenReady(model.adjust(
          Array(1),
          Array(Array(1)),
          Array(0.111f),
          Array(0.211f, 0.221f),
          seed)) {
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
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(vocabCns, 3, 0)
        val seed = 1

        var result = whenReady(model.adjust(
          Array(1, 0),
          Array(Array(0), Array(1)),
          Array(0.11f, 0.12f),
          Array(),
          seed)) {
          identity
        }
        assert(result)

        result = whenReady(model.adjust(
          Array(1, 0),
          Array(Array(0), Array(1)),
          Array(0.111f, 0.121f),
          Array(),
          seed)) {
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
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(vocabCns, 3, 2)
        val seed = 1

        // random negative words will be 2, 3, 3

        var result = whenReady(model.adjust(
          Array(1),
          Array(Array(0)),
          Array(0.11f),
          Array(0.21f, 0.22f),
          seed)) {
          identity
        }
        assert(result)

        result = whenReady(model.adjust(
          Array(1),
          Array(Array(0)),
          Array(0.111f),
          Array(0.211f, 0.221f),
          seed)) {
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
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(vocabCns, 3, 0)
        val seed = 1

        val value = whenReady(model.dotprod(
          Array(0),
          Array(Array(1, 0)),
          seed)) {
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
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(vocabCns, 3, 0)
        val seed = 1

        val result = whenReady(model.adjust(
          Array(1),
          Array(Array(1)),
          Array(0.11f),
          Array(),
          seed)) {
          identity
        }
        assert(result)

        val value = whenReady(model.dotprod(
          Array(1),
          Array(Array(1, 0)),
          seed)) {
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
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(vocabCns, 3, 2)
        val seed = 1

        val result = whenReady(model.adjust(
          Array(1),
          Array(Array(1)),
          Array(0.11f),
          Array(0.21f, 0.22f),
          seed)) {
          identity
        }
        assert(result)

        // random negative words will be 2, 3, 3, 2

        val value = whenReady(model.dotprod(
          Array(1),
          Array(Array(1, 0)),
          seed)) {
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
        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(vocabCns, 3, 0)

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

        val vocabCns = Array(3, 1, 4, 2)
        val model = client.word2vecMatrix(vocabCns, 3, 0)

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
}