package glint.vector

import glint.{FMPairArguments, HdfsTest, SystemTest, TolerantFloat}
import org.scalatest.{FlatSpec, Inspectors, Matchers}

import scala.math.{exp, sqrt}

/**
 * BigFMPairVector test specification
 */
class BigFMPairVectorSpec extends FlatSpec with SystemTest with HdfsTest with Matchers with Inspectors with TolerantFloat {

  val init = Array(0.046193548f, 0.066288196f, -0.051892724f, 0.021269038f, 0.02748347f, -0.038189888f, 0.010087393f)

  val args = FMPairArguments(k=7, batchSize=3)
  val featureProbs = Array(0.66f, 0.1f, 0.1f, 0.1f, 0.66f, 0.1f, 0.66f)
  val c = Array(0.4852f, 0.90333333333f, 0.90333333333f, 0.90333333333f, 0.4852f, 0.90333333333f, 0.4852f)

  "A BigFMPairVector" should "initialize values randomly" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val model = client.fmpairVector(args, featureProbs, hadoopConfig, 1)

        val values = whenReady(model.pull(Array(0, 1, 2, 3, 4, 5, 6))) { identity }

        values should equal(init)
      }
    }
  }

  it should "compute sums" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val model = client.fmpairVector(args, featureProbs, hadoopConfig, 1)

        val indices = Array(Array(0, 4, 6), Array(0, 5), Array(1, 3, 4, 6))
        val weights = Array(Array(1.0f, 1.0f, 0.3f), Array(1.0f, 1.0f), Array(1.0f, 0.25f, 1.0f, 0.3f))

        val (s, cacheKey) = whenReady(model.pullSum(indices, weights)) {
          identity
        }

        s should equal(Array(
          init(0) + init(4) + 0.3f * init(6),
          init(0) + init(5),
          init(1) + 0.25f * init(3) + init(4) + 0.3f * init(6)
        ))
        cacheKey should equal(0)
      }
    }
  }

  it should "adjust weights" in withMaster { _ =>
    withServers(3) { _ =>
      withClient { client =>
        val model = client.fmpairVector(args, featureProbs, hadoopConfig, 1)

        val indices = Array(Array(0, 4, 6), Array(0, 5), Array(1, 3, 4, 6))
        val weights = Array(Array(1.0f, 1.0f, 0.3f), Array(1.0f, 1.0f), Array(1.0f, 0.25f, 1.0f, 0.3f))

        val (s, cacheKey) = whenReady(model.pullSum(indices, weights)) {
          identity
        }

        val g = s.map(e => exp(-e)).map(e => (e / (1 + e)).toFloat)  // general BPR gradient

        val result = whenReady(model.pushSum(g, cacheKey)) {
          identity
        }
        assert(result)

        val values = whenReady(model.pull(Array(0, 1, 2, 3, 4, 5, 6))) { identity }

        values should equal(Array(
          init(0) + args.lr * c(0) * (g(0) + g(1) - 2 * args.linearReg * init(0)),
          init(1) + args.lr * c(1) * (g(2) - args.linearReg * init(1)),
          init(2),
          init(3) + args.lr * c(3) * (g(2) * 0.25f - args.linearReg * init(3)),
          init(4) + args.lr * c(4) * (g(0) + g(2) - 2 * args.linearReg * init(4)),
          init(5) + args.lr * c(5) * (g(1) - args.linearReg * init(5)),
          init(6) + args.lr * c(6) * (g(0) * 0.3f + g(2) * 0.3f - 2 * args.linearReg * init(6))
        ))
      }
    }
  }
}