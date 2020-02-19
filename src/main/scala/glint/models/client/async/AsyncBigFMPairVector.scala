package glint.models.client.async

import akka.actor.ActorRef
import com.github.fommil.netlib.F2jBLAS
import com.typesafe.config.Config
import glint.messages.server.request._
import glint.messages.server.response.ResponsePullSumFM
import glint.models.client.BigFMPairVector
import glint.partitioning.Partitioner
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
                      (implicit ec: ExecutionContext): Future[(Array[Float], Int)] = {

    // send pullSum pull request to the single partition
    val pullMessage = PullSumFM(keys, weights, cache)
    val fsm = PullFSM[PullSumFM, ResponsePullSumFM](pullMessage, models(0))
    val pull = fsm.run()

    pull.map(response => (response.s, response.cacheKey))
  }

  override def pushSum(g: Array[Float], cacheKey: Int)(implicit ec: ExecutionContext): Future[Boolean] = {

    require(trainable, "The vector has to be trainable to support pushSum")

    // Send adjust request to the single partition
    val pushMessage = PushSumFM(g, cacheKey)
    val fsm = PullFSM[PushSumFM, Boolean](pushMessage, models(0))
    val push = fsm.run()

    push.transform(result => true, err => err)
  }
}