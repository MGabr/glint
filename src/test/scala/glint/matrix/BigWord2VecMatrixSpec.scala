package glint.matrix

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import glint.SystemTest
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.{FlatSpec, Matchers}

class BigWord2VecMatrixSpec extends FlatSpec with SystemTest with Matchers {

  implicit val tolerantFloatEq: Equality[Float] = TolerantNumerics.tolerantFloatEquality(0.0000001f)

  implicit val tolerantFloatArrayEq: Equality[Array[Float]] = new Equality[Array[Float]] {
    override def areEqual(a: Array[Float], b: Any): Boolean = b match {
      case br: Array[Float] =>
        a.length == br.length && a.zip(br).forall { case (ax, bx) => tolerantFloatEq.areEqual(ax, bx) }
      case brr: Array[_] => a.deep == brr.deep
      case _ => a == b
    }
  }

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

        values should equal(Array(
          0.076989256f, 0.076959394f, 0.07704898f,
          0.11048033f, -0.13317561f, -0.0688745f,
          -0.08648787f, -0.02997307f, 0.13381587f,
          0.03544839f, -0.030853411f, -0.16528131f))
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

        values should equal(Array(
          0.076989256f, 0.076959394f, 0.07704898f,
          0.11048033f, -0.13317561f, -0.0688745f,
          -0.08648787f, -0.02997307f, 0.13381587f,
          0.03544839f, -0.030853411f, -0.16528131f))
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
          0.076989256f,
          0.076959394f,
          0.07704898f,
          0.11048033f + 0.111f * (0.11f * 0.11048033f) + 0.211f * (0.21f * 0.11048033f) + 0.221f * (0.22f * 0.11048033f),
          -0.13317561f + 0.111f * (0.11f * -0.13317561f) + 0.211f * (0.21f * -0.13317561f) + 0.221f * (0.22f * -0.13317561f),
          -0.0688745f + 0.111f * (0.11f * -0.0688745f) + 0.211f * (0.21f * -0.0688745f) + 0.221f * (0.22f * -0.0688745f),
          -0.08648787f,
          -0.02997307f,
          0.13381587f,
          0.03544839f,
          -0.030853411f,
          -0.16528131f))
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
          0.076989256f + 0.121f * (0.12f * 0.076989256f),
          0.076959394f + 0.121f * (0.12f * 0.076959394f),
          0.07704898f + 0.121f * (0.12f * 0.07704898f),
          0.11048033f + 0.111f * (0.11f * 0.11048033f),
          -0.13317561f + 0.111f * (0.11f * -0.13317561f),
          -0.0688745f + 0.111f * (0.11f * -0.0688745f),
          -0.08648787f,
          -0.02997307f,
          0.13381587f,
          0.03544839f,
          -0.030853411f,
          -0.16528131f))
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
          Array(0.11f, 0.12f),
          Array(0.21f, 0.22f),
          seed)) {
          identity
        }
        assert(result)

        result = whenReady(model.adjust(
          Array(1),
          Array(Array(0)),
          Array(0.111f, 0.121f),
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
          0.076989256f,
          0.076959394f,
          0.07704898f,
          0.11048033f + 0.111f * (0.11f * 0.11048033f) + 0.211f * (0.21f * 0.11048033f) + 0.221f * (0.22f * 0.11048033f),
          -0.13317561f + 0.111f * (0.11f * -0.13317561f) + 0.211f * (0.21f * -0.13317561f) + 0.221f * (0.22f * -0.13317561f),
          -0.0688745f + 0.111f * (0.11f * -0.0688745f) + 0.211f * (0.21f * -0.0688745f) + 0.221f * (0.22f * -0.0688745f),
          -0.08648787f,
          -0.02997307f,
          0.13381587f,
          0.03544839f,
          -0.030853411f,
          -0.16528131f))
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
          0.11048033f * (0.11f * 0.11048033f) - 0.13317561f * (0.11f * -0.13317561f) - 0.0688745f * (0.11f * -0.0688745f),
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
          0.11048033f * (0.11f * 0.11048033f) - 0.13317561f * (0.11f * -0.13317561f) - 0.0688745f * (0.11f * -0.0688745f),
          0f
        ))
        value._2 should equal(Array(
          0.11048033f * (0.21f * 0.11048033f) - 0.13317561f * (0.21f * -0.13317561f) - 0.0688745f * (0.21f * -0.0688745f),
          0.11048033f * (0.22f * 0.11048033f) - 0.13317561f * (0.22f * -0.13317561f) - 0.0688745f * (0.22f * -0.0688745f),
          0.11048033f * (0.22f * 0.11048033f) - 0.13317561f * (0.22f * -0.13317561f) - 0.0688745f * (0.22f * -0.0688745f),
          0.11048033f * (0.21f * 0.11048033f) - 0.13317561f * (0.21f * -0.13317561f) - 0.0688745f * (0.21f * -0.0688745f)
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
          blas.snrm2(3, Array(0.076989256f, 0.076959394f, 0.07704898f), 1),
          blas.snrm2(3, Array(0.11048033f, -0.13317561f, -0.0688745f), 1),
          blas.snrm2(3, Array(-0.08648787f, -0.02997307f, 0.13381587f), 1),
          blas.snrm2(3, Array(0.03544839f, -0.030853411f, -0.16528131f), 1)))
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

        val matrix = Array(
          0.076989256f, 0.076959394f, 0.07704898f,
          0.11048033f, -0.13317561f, -0.0688745f,
          -0.08648787f, -0.02997307f, 0.13381587f,
          0.03544839f, -0.030853411f, -0.16528131f)

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
