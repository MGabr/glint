package glint.spark

import glint.Client
import glint.exceptions.ServerCreationException
import org.scalatest.{Matchers, fixture}

class ClientSpec extends fixture.FlatSpec with fixture.TestDataFixture with SparkTest with Matchers {

  "A client" should "run on Spark" in withContext { sc =>
    val client = Client.runOnSpark(sc)()
    client.terminateOnSpark(sc)
  }

  it should "run on Spark with a parameter server per executor as default" in withContext { sc =>
    val client = Client.runOnSpark(sc)()
    try {
      val servers = whenReady(client.serverList())(identity)
      servers.length should equal(2)
    } finally {
      client.terminateOnSpark(sc)
    }
  }

  it should "run on Spark with a set number of parameter servers" in withContext { sc =>
    val client = Client.runOnSpark(sc)(numParameterServers = 1)
    try {
      val servers = whenReady(client.serverList())(identity)
      servers.length should equal(1)
    } finally {
      client.terminateOnSpark(sc)
    }
  }

  it should "not run on Spark with more parameter servers than executors" in withContext { sc =>
    an[ServerCreationException] should be thrownBy Client.runOnSpark(sc)(numParameterServers = 3)
  }


  it should "run with a Word2Vec matrix on Spark" in withContext { sc =>
    val vocabCns = (1 to 100).toArray
    val bcVocabCns = sc.broadcast(vocabCns)
    val (client, matrix) = Client.runWithWord2VecMatrixOnSpark(sc)(bcVocabCns, 300, 10)

    whenReady(matrix.destroy())(identity)
    client.terminateOnSpark(sc)
  }

  it should "run with a Word2Vec matrix on Spark with a parameter server per executor as default" in withContext { sc =>
    val vocabCns = Array(10, 11, 12, 13, 14, 15)
    val bcVocabCns = sc.broadcast(vocabCns)
    val (client, matrix) = Client.runWithWord2VecMatrixOnSpark(sc)(bcVocabCns, 10, 10, 1000)
    try {
      val servers = whenReady(client.serverList())(identity)
      servers.length should equal(2)
    } finally {

      whenReady(matrix.destroy())(identity)
      client.terminateOnSpark(sc)
    }
  }

  it should "run with a Word2Vec matrix on Spark with a set number of parameter servers" in withContext { sc =>
    val vocabCns = Array(10, 11, 12, 13, 14, 15)
    val bcVocabCns = sc.broadcast(vocabCns)
    val (client, matrix) = Client.runWithWord2VecMatrixOnSpark(sc)(bcVocabCns, 10, 10, 1000, numParameterServers = 1)
    try {
      val servers = whenReady(client.serverList())(identity)
      servers.length should equal(1)
    } finally {

      whenReady(matrix.destroy())(identity)
      client.terminateOnSpark(sc)
    }
  }

  it should "not run with a Word2Vec matrix on Spark with more parameter servers than executors" in withContext { sc =>
    val vocabCns = Array(10, 11, 12, 13, 14, 15)
    val bcVocabCns = sc.broadcast(vocabCns)
    an [ServerCreationException] should be thrownBy
      Client.runWithWord2VecMatrixOnSpark(sc)(bcVocabCns, 10, 10, 1000, numParameterServers = 3)
  }
}
