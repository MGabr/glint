package glint.spark

import com.typesafe.config.ConfigFactory
import glint.exceptions.ServerCreationException
import glint.{Client, Word2VecArguments}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Client integration test specification
  */
class ClientSpec extends FlatSpec with SparkTest with Matchers {

  val separateGlintConfig = ConfigFactory.parseResourcesAnySyntax("separate-glint.conf")
  val aeronConfig = ConfigFactory.parseResourcesAnySyntax("aeron-glint.conf")

  "A client" should "run on Spark" in {
    val client = Client.runOnSpark(sc)()
    client.terminateOnSpark(sc)
  }

  it should "run on Spark with a parameter server per executor as default" in {
    val client = Client.runOnSpark(sc)()
    try {
      val servers = whenReady(client.serverList())(identity)
      servers.length should equal(2)
    } finally {
      client.terminateOnSpark(sc)
    }
  }

  it should "run on Spark with a set number of parameter servers" in {
    val client = Client.runOnSpark(sc)(numParameterServers = 1)
    try {
      val servers = whenReady(client.serverList())(identity)
      servers.length should equal(1)
    } finally {
      client.terminateOnSpark(sc)
    }
  }

  it should "not run on Spark with more parameter servers than executors" in {
    an[ServerCreationException] should be thrownBy Client.runOnSpark(sc)(numParameterServers = 3)
  }


  it should "run with a Word2Vec matrix on Spark" in {
    val args = Word2VecArguments(300, 5, 50, 10)
    val vocabCns = (1 to 100).toArray
    val bcVocabCns = sc.broadcast(vocabCns)
    val (client, matrix) = Client.runWithWord2VecMatrixOnSpark(sc)(args, bcVocabCns)

    whenReady(matrix.destroy())(identity)
    client.terminateOnSpark(sc)
    bcVocabCns.destroy()
  }

  it should "run with a Word2Vec matrix on Spark with a parameter server per executor as default" in {
    val args = Word2VecArguments(10, 5, 50, 10, 1000)
    val vocabCns = Array(10, 11, 12, 13, 14, 15)
    val bcVocabCns = sc.broadcast(vocabCns)
    val (client, matrix) = Client.runWithWord2VecMatrixOnSpark(sc)(args, bcVocabCns)
    try {
      val servers = whenReady(client.serverList())(identity)
      servers.length should equal(2)
    } finally {
      whenReady(matrix.destroy())(identity)
      client.terminateOnSpark(sc)
      bcVocabCns.destroy()
    }
  }

  it should "run with a Word2Vec matrix on Spark with a set number of parameter servers" in {
    val args = Word2VecArguments(10, 5, 50, 10, 1000)
    val vocabCns = Array(10, 11, 12, 13, 14, 15)
    val bcVocabCns = sc.broadcast(vocabCns)
    val (client, matrix) = Client.runWithWord2VecMatrixOnSpark(sc)(args, bcVocabCns, numParameterServers = 1)
    try {
      val servers = whenReady(client.serverList())(identity)
      servers.length should equal(1)
    } finally {
      whenReady(matrix.destroy())(identity)
      client.terminateOnSpark(sc)
      bcVocabCns.destroy()
    }
  }

  it should "not run with a Word2Vec matrix on Spark with more parameter servers than executors" in {
    val args = Word2VecArguments(10, 5, 50, 10, 1000)
    val vocabCns = Array(10, 11, 12, 13, 14, 15)
    val bcVocabCns = sc.broadcast(vocabCns)
    try {
      an [ServerCreationException] should be thrownBy
        Client.runWithWord2VecMatrixOnSpark(sc)(args, bcVocabCns, numParameterServers = 3)
    } finally {
      bcVocabCns.destroy()
    }
  }

  it should "run connected to a Glint cluster in a separate Spark application" in {
    val client = Client(separateGlintConfig)
    try {
      val servers = whenReady(client.serverList())(identity)
      servers.length should equal(2)
    } finally {
      client.terminateOnSpark(sc, terminateOtherClients = true)
    }
  }

  it should "run on Spark using Aeron when specified" in {
    val client = Client.runOnSpark(sc, aeronConfig, Client.getNumExecutors(sc), Client.getExecutorCores(sc))
    try {
      val servers = whenReady(client.serverList())(identity)
      servers.length should equal(2)
    } finally {
      client.terminateOnSpark(sc)
    }
  }
}
