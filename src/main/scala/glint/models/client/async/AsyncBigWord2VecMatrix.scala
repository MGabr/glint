package glint.models.client.async

import akka.actor.ActorRef
import breeze.linalg.{DenseVector, Vector}
import breeze.numerics.sqrt
import com.github.fommil.netlib.F2jBLAS
import com.typesafe.config.Config
import glint.messages.server.request._
import glint.messages.server.response.{ResponseDotProd, ResponseFloat}
import glint.models.client.BigWord2VecMatrix
import glint.models.server.aggregate.Aggregate
import glint.partitioning.{Partition, Partitioner}
import glint.serialization.SerializableHadoopConfiguration
import org.apache.hadoop.conf.Configuration
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
  * @param trainable Whether the matrix is trainable
  */
class AsyncBigWord2VecMatrix(partitioner: Partitioner,
                             matrices: Array[ActorRef],
                             config: Config,
                             aggregate: Aggregate,
                             rows: Long,
                             cols: Long,
                             val n: Int,
                             val trainable: Boolean)
  extends AsyncBigMatrixFloat(partitioner, matrices, config, aggregate, rows, cols) with BigWord2VecMatrix {

  @transient
  private lazy val blas = new F2jBLAS

  private[glint] val numPartitions: Int = partitioner.all().length


  override def save(hdfsPath: String, hadoopConfig: Configuration)
                   (implicit ec: ExecutionContext): Future[Boolean] = {

    save(hdfsPath, hadoopConfig, true)
  }

  override def save(hdfsPath: String, hadoopConfig: Configuration, trainable: Boolean)
                   (implicit ec: ExecutionContext): Future[Boolean] = {

    if (trainable) {
      require(this.trainable, "The matrix has to be trainable to be saved as trainable")
    }

    // we don't have the metadata here
    // so the partial matrix which holds the first partition saves it
    val serHadoopConfig = new SerializableHadoopConfiguration(hadoopConfig)
    val pushes = partitioner.all().map {
      case partition =>
        val fsm = PushFSM[PushSaveTrainable](id =>
          PushSaveTrainable(id, hdfsPath, serHadoopConfig, trainable), matrices(partition.index))
        fsm.run()
    }.toIterator
    Future.sequence(pushes).transform(results => true, err => err)
  }

  override def dotprod(wInput: Array[Int], wOutput: Array[Array[Int]], seed: Long)
                      (implicit ec: ExecutionContext): Future[(Array[Float], Array[Float], Array[Int])] = {

    require(trainable, "The matrix has to be trainable to support dotprod")

    // Send dotprod pull requests to all partitions
    val pulls = partitioner.all().toIterable.map { partition =>
      val pullMessage = PullDotProd(wInput, wOutput, seed)
      val fsm = PullFSM[PullDotProd, ResponseDotProd](pullMessage, matrices(partition.index))
      fsm.run().map(r => (r, partition))
    }

    // Define aggregator for summing up partial dot products of successful responses
    def aggregateSuccess(responses: Iterable[(ResponseDotProd, Partition)]):
    (Array[Float], Array[Float], Array[Int]) = {

      val fLength = wOutput.map(_.length).sum
      val fPlusResults = new Array[Float](fLength)
      val fMinusResults = new Array[Float](fLength * n)
      val cacheKeys = new Array[Int](numPartitions)

      for ((response, partition) <- responses) {
        blas.saxpy(fLength, 1.0f, response.fPlus, 1, fPlusResults, 1)
        blas.saxpy(fLength * n, 1.0f, response.fMinus, 1, fMinusResults, 1)
        cacheKeys(partition.index) = response.cacheKey
      }

      (fPlusResults, fMinusResults, cacheKeys)
    }

    // Combine and aggregate futures
    Future.sequence(pulls).transform(aggregateSuccess, err => err)

  }

  override def adjust(gPlus: Array[Float], gMinus: Array[Float], cacheKeys: Array[Int])
                     (implicit ec: ExecutionContext): Future[Boolean] = {

    require(trainable, "The matrix has to be trainable to support adjust")

    // Send adjust requests to all partitions
    val pushes = partitioner.all().toIterable.map { partition =>
      val pushMessage = PushAdjust(gPlus, gMinus, cacheKeys(partition.index))
      val fsm = PullFSM[PushAdjust, Boolean](pushMessage, matrices(partition.index))
      fsm.run()
    }

    // Combine and aggregate futures
    Future.sequence(pushes).transform(results => true, err => err)

  }

  override def norms(startRow: Int = 0, endRow: Int = rows.toInt)
                    (implicit ec: ExecutionContext): Future[Array[Float]] = {

    // Send norm dots pull requests to all partitions
    val pulls = partitioner.all().toIterable.map { partition =>
      val pullMessage = PullNormDots(startRow, endRow)
      val fsm = PullFSM[PullNormDots, ResponseFloat](pullMessage, matrices(partition.index))
      fsm.run()
    }

    // Define aggregator for computing euclidean norms
    // by summing up partial dot products of successful responses and taking the square root
    def aggregateSuccess(responses: Iterable[ResponseFloat]): Array[Float] = {
      val lengthNorms = endRow - startRow
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

  override def multiply(vector: Array[Float], startRow: Int = 0, endRow: Int = rows.toInt)
                       (implicit ec: ExecutionContext): Future[Array[Float]] = {

    val keys = 0L until cols.toInt

    // Send pull request of the list of keys
    val pulls = mapPartitions(keys) {
      case (partition, indices) =>
        val pullMessage = PullMultiply(indices.map(vector).toArray, startRow, endRow)
        val fsm = PullFSM[PullMultiply, ResponseFloat](pullMessage, matrices(partition.index))
        fsm.run()
    }

    // Define aggregator for computing multiplication by summing up partial multiplication results
    def aggregateSuccess(responses: Iterable[ResponseFloat]): Array[Float] = {
      val lengthResult = endRow - startRow
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

  override def pullAverage(rows: Array[Array[Int]])(implicit ec: ExecutionContext): Future[Array[Vector[Float]]] = {

    // Send dotprod pull requests to all partitions
    val pulls = partitioner.all().toIterable.map { partition =>
      val pullMessage = PullAverageRows(rows)
      val fsm = PullFSM[PullAverageRows, ResponseFloat](pullMessage, matrices(partition.index))
      fsm.run().map(response => (response, partition))
    }

    // Define aggregator for successful responses
    def aggregateSuccess(responses: Iterable[(ResponseFloat, Partition)]): Array[Vector[Float]] = {
      val result = Array.fill(rows.length)(DenseVector.zeros[Float](cols.toInt).asInstanceOf[Vector[Float]])
      responses.foreach {
        case (response, partition) =>
          val partitionCols = partition.size
          cforRange(0 until result.length)(i => {
            cforRange(0 until partitionCols)(j => {
              result(i)(partition.localColToGlobal(j).toInt) = toValue(response, i * partitionCols + j)
            })
          })
      }
      result
    }

    // Combine and aggregate futures
    Future.sequence(pulls).transform(aggregateSuccess, err => err)
  }
}
