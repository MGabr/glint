package glint.models.server

import java.util.concurrent.ConcurrentHashMap

import akka.pattern.pipe
import glint.FMPairArguments
import glint.messages.server.request._
import glint.messages.server.response.{ResponseFloat, ResponsePullSumFM}
import glint.partitioning.Partition
import glint.serialization.SerializableHadoopConfiguration
import glint.util.hdfs
import glint.util.hdfs.FMPairMetadata
import org.eclipse.collections.api.block.procedure.primitive.IntFloatProcedure
import org.eclipse.collections.impl.map.mutable.primitive.{IntFloatHashMap, IntShortHashMap}
import spire.implicits.cforRange

import scala.concurrent.Future
import scala.math.sqrt
import scala.util.Random

/**
 * A partial vector holding floats
 *
 * @param partition The partition
 */
private[glint] class PartialVectorFMPair(partition: Partition,
                                         hdfsPath: Option[String],
                                         hadoopConfig: Option[SerializableHadoopConfiguration],
                                         val args: FMPairArguments,
                                         val numFeatures: Int,
                                         val avgActiveFeatures: Int,
                                         val trainable: Boolean)
  extends PartialVectorFloat(partition, hdfsPath,  hadoopConfig) {

  /**
   * The random number generator used for initializing the latent factors matrix
   */
  val random = new Random(partition.index)

  /**
   * The linear weights vector
   */
  var w: Array[Float] = _

  /**
   * The Adagrad buffers of the linear weights vector
   */
  var b: Array[Float] = _

  override def preStart(): Unit = {
    w = loadOrInitialize(Array.fill(size)(random.nextFloat() * 0.2f - 0.1f), pathPostfix = "/glint/data/w/")
    data = w

    if (trainable) {
      b = loadOrInitialize(Array.fill(size)(0.1f), pathPostfix = "/glint/data/wb/")
    }
  }


  /**
   * Thread local maps to avoid garbage collection
   */
  private val threadLocalUpdatesMap = new ThreadLocal[IntFloatHashMap] {
    override def initialValue() = new IntFloatHashMap(args.batchSize * avgActiveFeatures * 10)
  }

  private val threadLocalUpdateCountsMap = new ThreadLocal[IntShortHashMap] {
    override def initialValue(): IntShortHashMap = new IntShortHashMap(args.batchSize * avgActiveFeatures * 10)
  }


  /**
   * Update procedure which updates the weights according to the supplied partial gradient updates.
   * Uses an Adagrad learning rate, an adaptive learning rate for mini-batches with sparse gradients
   * and frequency adaptive L2 regularization.
   *
   * This is defined as thread local implementation to avoid the creation of an anonymous implementation
   * on each method call.
   */
  private val threadLocalUpdateProcedure = new ThreadLocal[IntFloatProcedure] {

    override def initialValue(): IntFloatProcedure = new IntFloatProcedure {

      private val updateCountsMap = threadLocalUpdateCountsMap.get()

      override def value(i: Int, wUpdateI: Float): Unit = {
        val wUpdateICount = updateCountsMap.get(i).toFloat
        w(i) += (wUpdateI - args.linearReg * w(i)) * args.lr / (sqrt(b(i)).toFloat * wUpdateICount)
        b(i) += (wUpdateI / wUpdateICount) * (wUpdateI / wUpdateICount)
      }
    }
  }


  /**
   * Cache to avoid duplicate transmission of indices and weights
   */
  private val cachePullSum = new ConcurrentHashMap[Int, (Array[Array[Int]], Array[Array[Float]])]()

  private var lastCacheKey = 0


  /**
   * Receives and handles incoming messages
   *
   * The specific messages for efficient distributed FM-Pair training (pullSum and pushSum) are executed
   * asynchronously and then send their result back to the sender. This means that we lose the Akka concurrency
   * guarantees and the dotprod and adjust methods access the actors state as shared mutable state
   * without any synchronization and locking. These methods can therefore overwrite each others gradient updates.
   *
   * This is, however, required to achieve good performance. As explained in papers like HOGWILD! this still achieves
   * a nearly optimal rate of convergence when the optimization problem is sparse.
   */
  override def receive: Receive = {
    case pull: PullVector =>
      sender ! ResponseFloat(get(pull.keys))
    case push: PushVectorFloat =>
      update(push.keys, push.values)
      updateFinished(push.id)
    case push: PushSaveTrainable =>
      save(push.path, push.hadoopConfig, push.trainable)
      updateFinished(push.id)
    case pull: PullSumFM =>
      val cacheKey = lastCacheKey
      lastCacheKey += 1
      Future {
        val s = pullSum(pull.indices, pull.weights, cacheKey, pull.cache)
        ResponsePullSumFM(s, cacheKey)
      } pipeTo sender()
    case push: PushSumFM =>
      Future {
        pushSum(push.g, push.cacheKey)
        true
      } pipeTo sender()
    case x =>
      handleLogic(x, sender)
  }

  def save(hdfsPath: String, hadoopConfig: SerializableHadoopConfiguration, saveTrainable: Boolean): Unit = {

    // the partial matrix holding the first partition also saves metadata
    if (partition.index == 0) {
      val meta = FMPairMetadata(args, numFeatures, avgActiveFeatures, trainable && saveTrainable)
      hdfs.saveFMPairMetadata(hdfsPath, hadoopConfig.conf, meta)
    }

    hdfs.savePartitionData(hdfsPath, hadoopConfig.conf, partition.index, w, pathPostfix = "/glint/data/w/")
    if (trainable && saveTrainable) {
      hdfs.savePartitionData(hdfsPath, hadoopConfig.conf, partition.index, b, pathPostfix = "/glint/data/wb/")
    }
  }

  /**
   * Pull the weighted partial sums of the feature indices
   *
   * @param indices The feature indices
   * @param weights The feature weights
   * @param cache Whether the indices and weights should be cached. Not required for recommendation
   * @return The weighted partial sums of the feature indices
   */
  def pullSum(indices: Array[Array[Int]], weights: Array[Array[Float]], cacheKey: Int, cache: Boolean): Array[Float] = {
    val s = new Array[Float](indices.length)

    cforRange(0 until indices.length)(i => {
      val ii = indices(i); val wi = weights(i)
      cforRange(0 until ii.length)(j => {
        s(i) += wi(j) * w(ii(j))
      })
    })

    if (cache) {
      cachePullSum.put(cacheKey, (indices, weights))
    }
    s
  }

  /**
   * Adjust the weights according to the received gradient updates
   *
   * @param g The BPR gradient per training instance in the batch
   * @param cacheKey The key to retrieve the cached indices and weights
   */
  def pushSum(g: Array[Float], cacheKey: Int): Unit = {

    // for asynchronous exactly-once delivery with PullFSM
    val cached = cachePullSum.remove(cacheKey)
    if (cached == null) {
      return
    }
    val (indices, weights) = cached

    // used to prevent garbage collection
    val wUpdates = threadLocalUpdatesMap.get()
    val wUpdateCounts = threadLocalUpdateCountsMap.get()

    cforRange(0 until indices.length)(i => {
      val ii = indices(i); val wi = weights(i); val gi = g(i)
      cforRange(0 until ii.length)(j => {
        wUpdates.addToValue(ii(j), gi * wi(j))
        wUpdateCounts.addToValue(ii(j), 1)
      })
    })

    wUpdates.forEachKeyValue(threadLocalUpdateProcedure.get())
    wUpdates.clear()
    wUpdateCounts.clear()
  }
}