package glint.spark

import glint.Client
import glint.models.client.granular.GranularBigWord2VecMatrix
import org.scalatest.{Inspectors, Matchers, fixture}

import scala.util.Random

class GranularBigWord2VecMatrixSpec extends fixture.FlatSpec with fixture.TestDataFixture with SparkTest
  with Matchers with Inspectors {

  "A GranularBigWord2VecMatrix" should "handle large norms requests" in withContext { sc =>
    val vocabCns = (0 to 1000000).toArray
    val bcVocabCns = sc.broadcast(vocabCns)
    val (client, model) = Client.runWithWord2VecMatrixOnSpark(sc)(bcVocabCns, 100, 5, 1000000)

    try {
      val granularModel = new GranularBigWord2VecMatrix(model, 10000)

      val result = whenReady(granularModel.norms()) {
        identity
      }

      result should have length vocabCns.length
    } finally {
      client.terminateOnSpark(sc)
    }
  }


  "A GranularBigWord2VecMatrix" should "handle large pullAverage requests" in withContext { sc =>
    val vocabCns = (0 to 50000).toArray
    val bcVocabCns = sc.broadcast(vocabCns)
    val (client, model) = Client.runWithWord2VecMatrixOnSpark(sc)(bcVocabCns, 100, 5, 1000000)

    try {
      val granularModel = new GranularBigWord2VecMatrix(model, 10000)
      val random = new Random(1)
      val rows = Array.fill(10000)(Array.fill(random.nextInt(15))(random.nextInt(vocabCns.length).toLong))

      val result = whenReady(granularModel.pullAverage(rows)) {
        identity
      }

      result should have length 10000
    } finally {
      client.terminateOnSpark(sc)
    }
  }
}
