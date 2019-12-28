package glint.models.client.async

import akka.actor.ActorRef
import com.github.fommil.netlib.F2jBLAS
import com.typesafe.config.Config
import glint.messages.server.request._
import glint.messages.server.response.ResponsePullSumFM
import glint.models.client.BigFMPairVector
import glint.partitioning.{Partition, Partitioner}
import glint.serialization.SerializableHadoopConfiguration
import org.apache.hadoop.conf.Configuration

import scala.concurrent.{ExecutionContext, Future}

class AsyncBigFMPairVector(partitioner: Partitioner,
                           models: Array[ActorRef],
                           config: Config,
                           numFeatures: Long,
                           val trainable: Boolean)
  extends AsyncBigVectorFloat(partitioner, models, config, numFeatures) with BigFMPairVector {

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
      require(this.trainable, "The vector has to be trainable to be saved as trainable")
    }

    // we don't have the metadata here
    // so the partial matrix which holds the first partition saves it
    val serHadoopConfig = new SerializableHadoopConfiguration(hadoopConfig)
    val pushes = partitioner.all().map {
      case partition =>
        val fsm = PushFSM[PushSaveTrainable](id =>
          PushSaveTrainable(id, hdfsPath, serHadoopConfig, trainable), models(partition.index))
        fsm.run()
    }.toIterator
    Future.sequence(pushes).transform(results => true, err => err)
  }

  override def pullSum(keys: Array[Array[Int]], weights: Array[Array[Float]], cache: Boolean = true)
                      (implicit ec: ExecutionContext): Future[(Array[Float], Array[Int])] = {

    // send pullSum pull requests to all partitions
    val pulls = partitioner.all().toIterable
      .map(partition => (partition, keys.map(subKeys => subKeys.indices.filter(i => partition.contains(subKeys(i))))))
      .map {
        case (partition, indices) =>

          val localKeys = indices.zipWithIndex.map { case (subIndices, i) =>
            subIndices.map(j => keys(i)(j).toLong).map(partition.globalRowToLocal).toArray
          }
          val localWeights = indices.zipWithIndex.map { case (subIndices, i) =>
            subIndices.map(j => weights(i)(j)).toArray
          }

          val pullMessage = PullSumFM(localKeys, localWeights, cache)
          val fsm = PullFSM[PullSumFM, ResponsePullSumFM](pullMessage, models(partition.index))
          fsm.run().map(r => (r, partition))
      }

    // Define aggregator for summing up partial sums of successful responses
    def aggregateSuccess(responses: Iterable[(ResponsePullSumFM, Partition)]): (Array[Float], Array[Int]) = {
      val sLength = keys.length
      val sResults = new Array[Float](sLength)
      val cacheKeys = new Array[Int](numPartitions)
      for ((response, partition) <- responses) {
        blas.saxpy(sLength, 1.0f, response.s, 1, sResults, 1)
        cacheKeys(partition.index) = response.cacheKey
      }
      (sResults, cacheKeys)
    }

    // Combine and aggregate futures
    Future.sequence(pulls).transform(aggregateSuccess, err => err)
  }

  override def pushSum(g: Array[Float], cacheKeys: Array[Int])(implicit ec: ExecutionContext): Future[Boolean] = {

    require(trainable, "The vector has to be trainable to support pushSum")

    // Send adjust requests to all partitions
    val pushes = partitioner.all().toIterable.map { partition =>
      val fsm = PushFSM[PushSumFM](id =>
        PushSumFM(id, g, cacheKeys(partition.index)), models(partition.index), parallelActor = true)
      fsm.run()
    }

    // Combine and aggregate futures
    Future.sequence(pushes).transform(results => true, err => err)
  }
}