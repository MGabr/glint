package glint.models.client.async

import akka.actor.ActorRef
import breeze.linalg.{DenseVector, Vector}
import breeze.numerics.sqrt
import com.github.fommil.netlib.F2jBLAS
import com.typesafe.config.Config
import glint.messages.server.request._
import glint.messages.server.response.ResponseDotProdFM
import glint.models.client.BigFMPairMatrix
import glint.models.server.aggregate.Aggregate
import glint.partitioning.{Partition, Partitioner}
import glint.serialization.SerializableHadoopConfiguration
import org.apache.hadoop.conf.Configuration
import spire.implicits.cforRange

import scala.concurrent.{ExecutionContext, Future}

class AsyncBigFMPairMatrix(partitioner: Partitioner,
                           matrices: Array[ActorRef],
                           config: Config,
                           aggregate: Aggregate,
                           rows: Long,
                           cols: Long,
                           val trainable: Boolean)
  extends AsyncBigMatrixFloat(partitioner, matrices, config, aggregate, rows, cols) with BigFMPairMatrix {

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

  override def dotprod(iUser: Array[Array[Int]],
                       wUser: Array[Array[Float]],
                       iItem: Array[Array[Int]],
                       wItem: Array[Array[Float]])
                      (implicit ec: ExecutionContext): Future[(Array[Float], Array[Int])] = {

    require(trainable, "The matrix has to be trainable to support dotprod")

    // Send dotprod pull requests to all partitions
    val pulls = partitioner.all().toIterable.map { partition =>
      val pullMessage = PullDotProdFM(iUser, wUser, iItem, wItem)
      val fsm = PullFSM[PullDotProdFM, ResponseDotProdFM](pullMessage, matrices(partition.index))
      fsm.run().map(r => (r, partition))
    }

    // Define aggregator for summing up partial dot products of successful responses
    def aggregateSuccess(responses: Iterable[(ResponseDotProdFM, Partition)]): (Array[Float], Array[Int]) = {
      val fLength = iUser.length
      val fResults = new Array[Float](fLength)
      val cacheKeys = new Array[Int](numPartitions)
      for ((response, partition) <- responses) {
        blas.saxpy(fLength, 1.0f, response.f, 1, fResults, 1)
        cacheKeys(partition.index) = response.cacheKey
      }
      (fResults, cacheKeys)
    }

    // Combine and aggregate futures
    Future.sequence(pulls).transform(aggregateSuccess, err => err)
  }

  override def adjust(g: Array[Float], cacheKeys: Array[Int])(implicit ec: ExecutionContext): Future[Boolean] = {

    require(trainable, "The matrix has to be trainable to support adjust")

    // Send adjust requests to all partitions
    val pushes = partitioner.all().toIterable.map { partition =>
      val fsm = PushFSM[PushAdjustFM](id =>
        PushAdjustFM(id, g, cacheKeys(partition.index)), matrices(partition.index), parallelActor = true)
      fsm.run()
    }

    // Combine and aggregate futures
    Future.sequence(pushes).transform(results => true, err => err)
  }
}