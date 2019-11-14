package glint.matrix

import breeze.linalg._
import com.github.fommil.netlib.F2jBLAS
import glint.{HdfsTest, SystemTest, Word2VecArguments, TolerantFloat}
import glint.FMPairArguments
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{FlatSpec, Inspectors, Matchers}

import scala.math.{exp, sqrt}

/**
 * BigWord2VecMatrix test specification
 */
class BigFMPairMatrixSpec extends FlatSpec with SystemTest with HdfsTest with Matchers with Inspectors with TolerantFloat {

  @transient
  private lazy val blas = new F2jBLAS

  val init =  Array(
    DenseVector(0.046193548f, 0.066288196f, 0.046175636f, -0.07990537f, 0.046229385f, -0.0413247f, 0.08028952f),
    DenseVector(-0.051892724f, 0.021269038f, -0.017983839f, -0.018512048f, -0.09916879f, -6.354898e-4f, 0.07093618f),
    DenseVector(0.02748347f, -0.038189888f, -0.058457043f, -0.09275293f, 0.09717538f, -0.09334294f, 0.071424805f),
    DenseVector(0.010087393f, -0.07659868f, -0.0334566f, 0.03177344f, 0.084624924f, 0.097484164f, -0.009428784f),
    DenseVector(0.019509055f, 0.05630692f, 0.09355117f, 0.042147927f, -0.054368425f, 0.003990218f, -0.08504124f),
    DenseVector(-0.033356324f, -0.049444772f, -0.09877657f, -0.06945276f, 0.06806279f, 0.04863154f, -0.05053109f),
    DenseVector(-0.022962168f, 0.022607148f, 0.09274096f, -0.0680844f, 0.089916654f, 0.010751128f, -0.09389736f)
  )

  "A BigFMPairMatrix" should "initialize values randomly" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val args = FMPairArguments(k=7, batchSize=3)
        val numFeatures = 7
        val model = client.fmpairMatrix(args, numFeatures)

        val values = whenReady(model.pull(Array(0, 1, 2, 3, 4, 5, 6))) { identity }

        values should equal(init)
      }
    }
  }

  it should "compute dot products" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val args = FMPairArguments(k=7, batchSize=3)
        val numFeatures = 7
        val model = client.fmpairMatrix(args, numFeatures)

        val iUser = Array(Array(0), Array(0), Array(1, 3))
        val wUser = Array(Array(1.0f), Array(1.0f), Array(1.0f, 0.25f))
        val iItem = Array(Array(4, 6), Array(5), Array(4, 6))
        val wItem = Array(Array(1.0f, 0.3f), Array(1.0f), Array(1.0f, 0.3f))

        val (f, cacheKeys) = whenReady(model.dotprod(iUser, wUser, iItem, wItem)) {
          identity
        }

        // breeze "x dot y" instead of "sum(x *:* y)" seems to have floating point precision problems and returns 0.0
        f should equal(Array(
          sum(init(0) *:* (init(4) + 0.3f * init(6))),
          sum(init(0) *:* init(5)),
          sum((init(1) + 0.25f * init(3)) *:* (init(4) + 0.3f * init(6)))
        ))
        cacheKeys should equal(Array(0, 0, 0))
      }
    }
  }

  it should "adjust weights" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val args = FMPairArguments(k=7, batchSize=3)
        val numFeatures = 7
        val model = client.fmpairMatrix(args, numFeatures)

        val iUser = Array(Array(0), Array(0), Array(1, 3))
        val wUser = Array(Array(1.0f), Array(1.0f), Array(1.0f, 0.25f))
        val iItem = Array(Array(4, 6), Array(5), Array(4, 6))
        val wItem = Array(Array(1.0f, 0.3f), Array(1.0f), Array(1.0f, 0.3f))

        val (f, cacheKeys) = whenReady(model.dotprod(iUser, wUser, iItem, wItem)) {
          identity
        }

        val g = f.map(e => exp(-e)).map(e => (e / (1 + e)).toFloat)  // general BPR gradient

        val result = whenReady(model.adjust(g, cacheKeys)) {
          identity
        }
        assert(result)

        val values = whenReady(model.pull(Array(0, 1, 2, 3, 4, 5, 6))) { identity }

        val ada = sqrt(0.1 + 1e-07).toFloat  // initial Adagrad learning rate

        values should equal(Array(
          init(0) + args.lr / ada * (g(0) * (init(4) + 0.3f * init(6)) + g(1) * init(5) - args.factorsReg * init(0)),
          init(1) + args.lr / ada * (g(2) * (init(4) + 0.3f * init(6)) - args.factorsReg * init(1)),
          init(2),
          init(3) + args.lr / ada * (0.25f * g(2) * (init(4) + 0.3f * init(6)) - args.factorsReg * init(3)),
          init(4) + args.lr / ada * (g(0) * init(0) + g(2) * (init(1) + 0.25f * init(3)) - args.factorsReg * init(4)),
          init(5) + args.lr / ada * (g(1) * init(0) - args.factorsReg * init(5)),
          init(6) + args.lr / ada * (0.3f * (g(0) * init(0) + g(2) * (init(1) + 0.25f * init(3))) - args.factorsReg * init(6))
        ))
      }
    }
  }
}