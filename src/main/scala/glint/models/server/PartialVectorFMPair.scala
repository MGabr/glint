package glint.models.server

import java.util.concurrent.ConcurrentHashMap

import akka.pattern.pipe
import glint.FMPairArguments
import glint.messages.server.logic.AcknowledgeReceipt
import glint.messages.server.request._
import glint.messages.server.response.{ResponsePullSumFM, ResponseFloat, ResponseRowsFloat}
import glint.models.server.aggregate.Aggregate
import glint.partitioning.Partition
import glint.serialization.SerializableHadoopConfiguration
import glint.util.hdfs.FMPairMetadata
import glint.util.{FloatArrayPool, IntArrayPool, hdfs}
import spire.implicits.cforRange

import scala.collection.mutable
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

  private val threadLocalUpdatesArrayPool = new ThreadLocal[FloatArrayPool] {
    override def initialValue(): FloatArrayPool = new FloatArrayPool(size)
  }

  private val threadLocalIndicesArrayPool = new ThreadLocal[IntArrayPool] {
    override def initialValue(): IntArrayPool = new IntArrayPool(args.batchSize * avgActiveFeatures)
  }

  private val cache = new ConcurrentHashMap[Int, (Array[Array[Int]], Array[Array[Float]])]()

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
    case pull: PullVector => sender ! ResponseFloat(get(pull.keys))
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
        val s = pullSum(pull.indices, pull.weights, cacheKey)
        ResponsePullSumFM(s, cacheKey)
      } pipeTo sender()
    case push: PushAdjustFM =>
      Future {
        adjust(push.g, push.cacheKey)
        updateFinished(push.id)
        AcknowledgeReceipt(push.id)
      } pipeTo sender()
    case x => handleLogic(x, sender)
  }

  /**
   * A synchronized set of received message ids.
   * Required since pushSum messages are handled asynchronously without synchronization
   */
  override val receipt: mutable.HashSet[Int] = new mutable.HashSet[Int] with mutable.SynchronizedSet[Int]

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

  def pullSum(indices: Array[Array[Int]], weights: Array[Array[Float]], cacheKey: Int): Array[Float] = {
    val s = new Array[Float](indices.length)

    cforRange(0 until indices.length)(i => {
      val ii = indices(i); val wi = weights(i)
      cforRange(0 until ii.length)(j => {
        s(i) += wi(j) * w(ii(j))
      })
    })

    cache.put(cacheKey, (indices, weights))
    s
  }

  def adjust(g: Array[Float], cacheKey: Int): Unit = {
    val (indices, weights) = cache.get(cacheKey)
    cache.remove(cacheKey)

    // used to prevent garbage collection
    val updatesArrayPool = threadLocalUpdatesArrayPool.get()
    val indicesArrayPool = threadLocalIndicesArrayPool.get()

    // partial gradient updates to be applied at the end
    val wUpdates = updatesArrayPool.get()

    // indices of partial gradient updates to be applied
    var indicesLength = 0
    cforRange(0 until indices.length)(i => {
      indicesLength += indices(i).length
    })
    val wIndices = indicesArrayPool.get(indicesLength)
    var wIndex = 0

    cforRange(0 until indices.length)(i => {
      val ii = indices(i); val wi = weights(i); val gi = g(i)
      cforRange(0 until ii.length)(j => {
        if (wUpdates(ii(j)) == 0.0f) {
          wIndices(wIndex) = ii(j)
          wIndex += 1
        }
        wUpdates(ii(j)) += gi * wi(j)
      })
    })

    // apply partial gradient updates
    cforRange(0 until wIndex)(wi => {
      val i = wIndices(wi)
      w(i) += (wUpdates(i) - args.linearReg * w(i)) * args.lr / sqrt(b(i) + 1e-07).toFloat
      b(i) += wUpdates(i) * wUpdates(i)
      wUpdates(i) = 0.0f
    })

    updatesArrayPool.put(wUpdates)
    indicesArrayPool.putClearUntil(wIndices, wIndex)
  }
}