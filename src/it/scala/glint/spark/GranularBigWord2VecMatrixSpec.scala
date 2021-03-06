package glint.spark

import glint.{Client, Word2VecArguments}
import glint.models.client.granular.GranularBigWord2VecMatrix
import org.scalatest._

import scala.util.Random

/**
  * GranularBigWord2VecMatrix integration test specification
  */
class GranularBigWord2VecMatrixSpec extends FlatSpec with SparkTest with Matchers {

  "A GranularBigWord2VecMatrix" should "handle large norms responses" in {
    val args = Word2VecArguments(100, 5, 50, 10, 1000000)
    val vocabCns = (0 to 500000).toArray
    val bcVocabCns = sc.broadcast(vocabCns)
    val (client, model) = Client.runWithWord2VecMatrixOnSpark(sc)(args, bcVocabCns)

    try {
      val granularModel = new GranularBigWord2VecMatrix(model, 10000)

      val result = whenReady(granularModel.norms()) {
        identity
      }

      result should have length vocabCns.length
    } finally {
      client.terminateOnSpark(sc)
      bcVocabCns.destroy()
    }
  }

  it should "handle large pullAverage requests" in {
    val args = Word2VecArguments(100, 5, 50, 10, 1000000)
    val vocabCns = (0 to 50000).toArray
    val bcVocabCns = sc.broadcast(vocabCns)
    val (client, model) = Client.runWithWord2VecMatrixOnSpark(sc)(args, bcVocabCns)

    try {
      val granularModel = new GranularBigWord2VecMatrix(model, 10000)
      val random = new Random(1)
      val rows = Array.fill(10000)(Array.fill(random.nextInt(15))(random.nextInt(vocabCns.length)))

      val result = whenReady(granularModel.pullAverage(rows)) {
        identity
      }

      result should have length 10000
    } finally {
      client.terminateOnSpark(sc)
      bcVocabCns.destroy()
    }
  }

  it should "handle large multiply responses" in {
    val args = Word2VecArguments(100, 5, 50, 10, 1000000)
    val vocabCns = (0 to 500000).toArray
    val bcVocabCns = sc.broadcast(vocabCns)
    val (client, model) = Client.runWithWord2VecMatrixOnSpark(sc)(args, bcVocabCns)

    try {
      val granularModel = new GranularBigWord2VecMatrix(model, 10000)
      val vector = (0 until 100).map(_.toFloat).toArray

      val result = whenReady(granularModel.multiply(vector)) {
        identity
      }

      result should have length vocabCns.length
    } finally {
      client.terminateOnSpark(sc)
      bcVocabCns.destroy()
    }
  }
}
