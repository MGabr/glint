package glint.models.client.async

import akka.actor.ActorRef
import breeze.numerics.sqrt
import com.github.fommil.netlib.F2jBLAS
import com.typesafe.config.Config
import glint.messages.server.request.{PullDotProd, PullMultiply, PullNormDots, PushAdjust}
import glint.messages.server.response.{ResponseDotProd, ResponseFloat}
import glint.models.client.BigWord2VecMatrix
import glint.models.server.aggregate.Aggregate
import glint.partitioning.Partitioner
import spire.implicits.cforRange

import scala.concurrent.{ExecutionContext, Future}

/**
  * An asynchronous implementation of a [[glint.models.client.BigWord2VecMatrix BigWord2VecMatrix]].
  * You don't want to construct this object manually but instead use the methods provided in [[glint.Client Client]]
  * to run them on Spark, as so
  *
  * {{{
  *   Client.runWithWord2VecMatrixOnSpark(sc, bcVocabCns, vectorSize, n)
  * }}}
  *
  * @param partitioner A partitioner to map rows to partitions
  * @param matrices The references to the partial matrices on the parameter servers
  * @param config The glint configuration (used for serialization/deserialization construction of actorsystems)
  * @param aggregate The type of aggregation to perform on this model (used only for saving this parameter)
  * @param rows The number of rows
  * @param cols The number of columns
  * @param n The number of random negative words for Word2Vec
  */
class AsyncBigWord2VecMatrix(partitioner: Partitioner,
                             matrices: Array[ActorRef],
                             config: Config,
                             aggregate: Aggregate,
                             rows: Long,
                             cols: Long,
                             val n: Int)
  extends AsyncBigMatrixFloat(partitioner, matrices, config, aggregate, rows, cols) with BigWord2VecMatrix {

  @transient
  private lazy val blas = new F2jBLAS

  override def dotprod(wInput: Array[Int], wOutput: Array[Array[Int]], seed: Long)
                      (implicit ec: ExecutionContext): Future[(Array[Float], Array[Float])] = {

    // Send dotprod pull requests to all partitions
    val pulls = partitioner.all().toIterable.map { partition =>
      val pullMessage = PullDotProd(wInput, wOutput, seed)
      val fsm = PullFSM[PullDotProd, ResponseDotProd](pullMessage, matrices(partition.index))
      fsm.run()
    }

    // Define aggregator for summing up partial dot products of successful responses
    def aggregateSuccess(responses: Iterable[ResponseDotProd]): (Array[Float], Array[Float]) = {
      val responseLength = wOutput.length * n

      val length = wOutput.map(_.length).sum

      val fPlusResults = new Array[Float](length)
      val fMinusResults = new Array[Float](length * n)

      val responsesArray = responses.toArray
      cforRange(0 until responsesArray.length)(i => {
        blas.saxpy(length, 1.0f, responsesArray(i).fPlus, 1, fPlusResults, 1)
        blas.saxpy(length * n, 1.0f, responsesArray(i).fMinus, 1, fMinusResults, 1)
      })

      (fPlusResults, fMinusResults)
    }

    // Combine and aggregate futures
    Future.sequence(pulls).transform(aggregateSuccess, err => err)

  }

  override def adjust(wInput: Array[Int],
                      wOutput: Array[Array[Int]],
                      gPlus: Array[Float],
                      gMinus: Array[Float],
                      seed: Long)(implicit ec: ExecutionContext): Future[Boolean] = {

    // Send adjust requests to all partitions
    val pushes = partitioner.all().toIterable.map { partition =>
      val fsm = PushFSM[PushAdjust](id => PushAdjust(id, wInput, wOutput, gPlus, gMinus, seed), matrices(partition.index))
      fsm.run()
    }

    // Combine and aggregate futures
    Future.sequence(pushes).transform(results => true, err => err)

  }

  override def norms()(implicit ec: ExecutionContext): Future[Array[Float]] = {

    // Send norm dots pull requests to all partitions
    val pulls = partitioner.all().toIterable.map { partition =>
      val pullMessage = PullNormDots()
      val fsm = PullFSM[PullNormDots, ResponseFloat](pullMessage, matrices(partition.index))
      fsm.run()
    }

    // Define aggregator for computing euclidean norms
    // by summing up partial dot products of successful responses and taking the square root
    def aggregateSuccess(responses: Iterable[ResponseFloat]): Array[Float] = {
      val lengthNorms = rows.toInt
      val norms = new Array[Float](lengthNorms)

      val responsesArray = responses.toArray
      cforRange(0 until responsesArray.length)(i => {
        blas.saxpy(lengthNorms, 1.0f, responsesArray(i).values, 1, norms, 1)
      })

      norms.map(x => sqrt(x))
    }

    // Combine and aggregate futures
    Future.sequence(pulls).transform(aggregateSuccess, err => err)

  }

  override def multiply(vector: Array[Float])(implicit ec: ExecutionContext): Future[Array[Float]] = {

    val keys = 0L until cols.toInt

    // Send pull request of the list of keys
    val pulls = mapPartitions(keys) {
      case (partition, indices) =>
        val pullMessage = PullMultiply(indices.map(vector).toArray)
        val fsm = PullFSM[PullMultiply, ResponseFloat](pullMessage, matrices(partition.index))
        fsm.run()
    }

    // Define aggregator for computing multiplication by summing up partial multiplication results
    def aggregateSuccess(responses: Iterable[ResponseFloat]): Array[Float] = {
      val lengthResult = rows.toInt
      val result = new Array[Float](lengthResult)

      val responsesArray = responses.toArray
      cforRange(0 until responsesArray.length)(i => {
        blas.saxpy(lengthResult, 1.0f, responsesArray(i).values, 1, result, 1)
      })

      result
    }

    // Combine and aggregate futures
    Future.sequence(pulls).transform(aggregateSuccess, err => err)
  }
}
