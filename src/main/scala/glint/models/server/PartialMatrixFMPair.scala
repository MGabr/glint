package glint.models.server

import java.util.concurrent.ConcurrentHashMap

import akka.pattern.pipe
import com.github.fommil.netlib.F2jBLAS
import glint.FMPairArguments
import glint.messages.server.logic.AcknowledgeReceipt
import glint.messages.server.request._
import glint.messages.server.response.{ResponseDotProdFM, ResponseFloat, ResponsePullSumFM, ResponseRowsFloat}
import glint.models.server.aggregate.Aggregate
import glint.partitioning.Partition
import glint.serialization.SerializableHadoopConfiguration
import glint.util.hdfs.FMPairMetadata
import glint.util.{FloatArrayPool, FloatArraysArrayPool, IntArrayPool, hdfs}
import spire.implicits.cforRange

import scala.collection.mutable
import scala.concurrent.Future
import scala.math.sqrt
import scala.util.Random

/**
  * A partial matrix holding floats and supporting specific messages for efficient distributed FM-Pair training
  *
  * @param partition The partition
  * @param aggregate The type of aggregation to apply
  * @param hdfsPath The HDFS base path from which the partial matrix' initial data should be loaded from
  * @param hadoopConfig The serializable Hadoop configuration to use for loading the initial data from HDFS
  * @param args The [[glint.FMPairArguments FMPairArguments]]
  * @param numFeatures The number of features
  * @param avgActiveFeatures The average number of active features. Not an important parameter but used for
  *                          determining good array pool sizes against garbage collection.
  * @param trainable Whether the matrix is trainable, requiring more data (Adagrad buffers)
  */
private[glint] class PartialMatrixFMPair(partition: Partition,
                                         aggregate: Aggregate,
                                         hdfsPath: Option[String],
                                         hadoopConfig: Option[SerializableHadoopConfiguration],
                                         val args: FMPairArguments,
                                         val numFeatures: Int,
                                         val avgActiveFeatures: Int,
                                         val trainable: Boolean)
  extends PartialMatrixFloat(partition, numFeatures, partition.size, aggregate, hdfsPath, hadoopConfig) {

  @transient
  private lazy val blas = new F2jBLAS

  /**
    * The random number generator used for initializing the latent factors matrix
    */
  val random = new Random(partition.index)

  /**
    * The latent factors matrix
    */
  var v: Array[Float] = _

  /**
    * The Adagrad buffers of the latent factors matrix
    */
  var b: Array[Float] = _

  override def preStart(): Unit = {
    v = loadOrInitialize(Array.fill(rows * cols)(random.nextFloat() * 0.2f - 0.1f), pathPostfix = "/glint/data/v/")
    data = v

    if (trainable) {
      b = loadOrInitialize(Array.fill(rows * cols)(0.1f), pathPostfix = "/glint/data/b/")
    }
  }

  private val threadLocalUpdateArraysArrayPool = new ThreadLocal[FloatArraysArrayPool] {
    override def initialValue(): FloatArraysArrayPool = new FloatArraysArrayPool(rows)
  }

  private val threadLocalUpdatesArrayPool = new ThreadLocal[FloatArrayPool] {
    override def initialValue(): FloatArrayPool = new FloatArrayPool(cols)
  }

  private val threadLocalSumsArrayPool = new ThreadLocal[FloatArrayPool] {
    override def initialValue(): FloatArrayPool = new FloatArrayPool(args.batchSize * cols)
  }

  private val threadLocalIndicesArrayPool = new ThreadLocal[IntArrayPool] {
    override def initialValue(): IntArrayPool = new IntArrayPool(args.batchSize * avgActiveFeatures)
  }

  private val cacheDotProd = new ConcurrentHashMap[
    Int,
    (Array[Array[Int]], Array[Array[Float]], Array[Float], Array[Array[Int]], Array[Array[Float]], Array[Float])]()

  private val cachePullSum = new ConcurrentHashMap[Int, (Array[Array[Int]], Array[Array[Float]])]()

  private var lastCacheKey = 0

  /**
    * Receives and handles incoming messages
    *
    * The specific messages for efficient distributed FM-Pair training (dotprod and adjust) are executed
    * asynchronously and then send their result back to the sender. This means that we lose the Akka concurrency
    * guarantees and the dotprod and adjust methods access the actors state as shared mutable state
    * without any synchronization and locking. These methods can therefore overwrite each others gradient updates.
    *
    * This is, however, required to achieve good performance. As explained in papers like HOGWILD! this still achieves
    * a nearly optimal rate of convergence when the optimization problem is sparse.
    */
  override def receive: Receive = {
    case pull: PullMatrix =>
      sender ! ResponseFloat(get(pull.rows, pull.cols))
    case pull: PullMatrixRows =>
      sender ! ResponseRowsFloat(getRows(pull.rows), cols)
    case push: PushMatrixFloat =>
      update(push.rows, push.cols, push.values)
      updateFinished(push.id)
    case push: PushSaveTrainable =>
      save(push.path, push.hadoopConfig, push.trainable)
      updateFinished(push.id)
    case pull: PullDotProdFM =>
      val cacheKey = lastCacheKey
      lastCacheKey += 1
      Future {
        val f = dotprod(pull.iUser, pull.wUser, pull.iItem, pull.wItem, cacheKey, pull.cache)
        ResponseDotProdFM(f, cacheKey)
      } pipeTo sender()
    case push: PushAdjustFM =>
      Future {
        adjust(push.g, push.cacheKey)
        updateFinished(push.id)
        AcknowledgeReceipt(push.id)
      } pipeTo sender()
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
        updateFinished(push.id)
        AcknowledgeReceipt(push.id)
      } pipeTo sender()
    case x =>
      handleLogic(x, sender)
  }

  /**
    * A synchronized set of received message ids.
    * Required since pushAdjust messages are handled asynchronously without synchronization
    */
  override val receipt: mutable.HashSet[Int] = new mutable.HashSet[Int] with mutable.SynchronizedSet[Int]


  def save(hdfsPath: String, hadoopConfig: SerializableHadoopConfiguration, saveTrainable: Boolean): Unit = {

    // the partial matrix holding the first partition also saves metadata
    if (partition.index == 0) {
      val meta = FMPairMetadata(args, numFeatures, avgActiveFeatures, trainable && saveTrainable)
      hdfs.saveFMPairMetadata(hdfsPath, hadoopConfig.conf, meta)
    }

    hdfs.savePartitionData(hdfsPath, hadoopConfig.conf, partition.index, v, pathPostfix = "/glint/data/v/")
    if (trainable && saveTrainable) {
      hdfs.savePartitionData(hdfsPath, hadoopConfig.conf, partition.index, b, pathPostfix = "/glint/data/b/")
    }
  }

  /**
    * Computes the partial dot products
    *
    * @param iUser The user feature indices
    * @param wUser The user feature weights
    * @param iItem The item feature indices
    * @param wItem The item feature weights
    * @param cacheKey The key to cache the indices and weights
    * @param cache Whether the indices and weights should be cached
    * @return The partial dot products
    */
  def dotprod(iUser: Array[Array[Int]],
              wUser: Array[Array[Float]],
              iItem: Array[Array[Int]],
              wItem: Array[Array[Float]],
              cacheKey: Int,
              cache: Boolean): Array[Float] = {

    // used to prevent garbage collection
    val sumsArrayPool = threadLocalSumsArrayPool.get()
    val sUser = sumsArrayPool.get()
    val sItem = sumsArrayPool.get()

    val f = new Array[Float](iUser.length)

    cforRange(0 until iUser.length)(i => {
      val iu = iUser(i); val wu = wUser(i)
      val ii = iItem(i); val wi = wItem(i)
      val sOffset = i * cols
      cforRange(0 until iu.length)(j => {
        blas.saxpy(cols, wu(j), v, iu(j) * cols, 1, sUser, sOffset, 1)
      })
      cforRange(0 until ii.length)(j => {
        blas.saxpy(cols, wi(j), v, ii(j) * cols, 1, sItem, sOffset, 1)
      })
      f(i) = blas.sdot(cols, sUser, sOffset, 1, sItem, sOffset, 1)
    })

    if (cache) {
      cacheDotProd.put(cacheKey, (iUser, wUser, sUser, iItem, wItem, sItem))
    } else {
      sumsArrayPool.putClear(sUser)
      sumsArrayPool.putClear(sItem)
    }
    f
  }

  /**
    * Adjusts the weights according to the received gradient updates.
    * Uses an Adagrad learning rate and frequency adaptive L2 regularization
    *
    * @param g The general BPR gradient per training instance in the batch
    * @param cacheKey The key to retrieve the cached indices and weights
    */
  def adjust(g: Array[Float], cacheKey: Int): Unit = {
    val (iUser, wUser, sUser, iItem, wItem, sItem) = cacheDotProd.get(cacheKey)
    cacheDotProd.remove(cacheKey)

    // used to prevent garbage collection
    val sumsArrayPool = threadLocalSumsArrayPool.get()
    val updateArraysArrayPool = threadLocalUpdateArraysArrayPool.get()
    val updatesArrayPool = threadLocalUpdatesArrayPool.get()
    val indicesArrayPool = threadLocalIndicesArrayPool.get()

    // matrix holding partial gradient updates to be applied at the end
    // only create arrays on demand for rows which are updated
    val vUpdates = updateArraysArrayPool.get()

    // indices of partial gradient updates to be applied
    var indicesLength = 0
    cforRange(0 until iUser.length)(i => {
      indicesLength += iUser(i).length + iItem(i).length
    })
    val vIndices = indicesArrayPool.get(indicesLength)
    var vIndex = 0

    cforRange(0 until iUser.length)(i => {
      val iu = iUser(i); val wu = wUser(i)
      val ii = iItem(i); val wi = wItem(i)
      val sOffset = i * cols
      val gb = g(i)

      cforRange(0 until iu.length)(j => {
        if (vUpdates(iu(j)) == null) {
          vUpdates(iu(j)) = updatesArrayPool.get()
          vIndices(vIndex) = iu(j)
          vIndex += 1
        }
        blas.saxpy(cols, gb * wu(j), sItem, sOffset, 1, vUpdates(iu(j)), 0, 1)
      })

      cforRange(0 until ii.length)(j => {
        if (vUpdates(ii(j)) == null) {
          vUpdates(ii(j)) = updatesArrayPool.get()
          vIndices(vIndex) = ii(j)
          vIndex += 1
        }
        blas.saxpy(cols, gb * wi(j), sUser, sOffset, 1, vUpdates(ii(j)), 0, 1)
      })
    })

    sumsArrayPool.putClear(sUser)
    sumsArrayPool.putClear(sItem)

    // apply partial gradient updates
    cforRange(0 until vIndex)(vi => {
      val i = vIndices(vi)
      val vUpdatesI = vUpdates(i)
      val colsi = cols * i
      cforRange(0 until cols)(j => {
        val offsetJ = colsi + j
        v(offsetJ) += (vUpdatesI(j) - args.factorsReg * v(offsetJ)) * args.lr / sqrt(b(offsetJ) + 1e-07).toFloat
        b(offsetJ) += vUpdatesI(j) * vUpdatesI(j)
      })

      updatesArrayPool.putClear(vUpdates(i))
      vUpdates(i) = null
    })

    updateArraysArrayPool.put(vUpdates)
    indicesArrayPool.putClearUntil(vIndices, vIndex)
  }

  /**
   * Pull the weighted partial sums of the feature indices
   *
   * @param indices The feature indices
   * @param weights The feature weights
   * @param cacheKey The key to cache the indices and weights
   * @param cache Whether the indices, weights and sums should be cached
   * @return A future containing the weighted sums of the feature indices
   */
  def pullSum(indices: Array[Array[Int]], weights: Array[Array[Float]], cacheKey: Int, cache: Boolean): Array[Float] = {
    val s = new Array[Float](indices.length * cols)

    cforRange(0 until indices.length)(i => {
      val ii = indices(i); val wi = weights(i)
      val sOffset = i * cols
      cforRange(0 until ii.length)(j => {
        blas.saxpy(cols, wi(j), v, ii(j) * cols, 1, s, sOffset, 1)
      })
    })

    if (cache) {
      cachePullSum.put(cacheKey, (indices, weights))
    }
    s
  }

  /**
   * Adjusts the weights according to the received partial sum gradient updates
   *
   * @param g The partial BPR gradients per training instance in the batch
   * @param cacheKey The key to retrieve the cached indices and weights
   */
  def pushSum(g: Array[Float], cacheKey: Int): Unit = {
    val (indices, weights) = cachePullSum.get(cacheKey)
    cachePullSum.remove(cacheKey)

    // used to prevent garbage collection
    val updateArraysArrayPool = threadLocalUpdateArraysArrayPool.get()
    val updatesArrayPool = threadLocalUpdatesArrayPool.get()
    val indicesArrayPool = threadLocalIndicesArrayPool.get()

    // matrix holding partial gradient updates to be applied at the end
    // only create arrays on demand for rows which are updated
    val vUpdates = updateArraysArrayPool.get()

    // indices of partial gradient updates to be applied
    var indicesLength = 0
    cforRange(0 until indices.length)(i => {
      indicesLength += indices(i).length
    })
    val vIndices = indicesArrayPool.get(indicesLength)
    var vIndex = 0

    cforRange(0 until indices.length)(i => {
      val ii = indices(i); val wi = weights(i)
      val gOffset = i * cols
      cforRange(0 until ii.length)(j => {
        if (vUpdates(ii(j)) == null) {
          vUpdates(ii(j)) = updatesArrayPool.get()
          vIndices(vIndex) = ii(j)
          vIndex += 1
        }
        blas.saxpy(cols, wi(j), g, gOffset, 1, vUpdates(ii(j)), 0, 1)
      })
    })

    // apply partial gradient updates
    cforRange(0 until vIndex)(vi => {
      val i = vIndices(vi)
      val vUpdatesI = vUpdates(i)
      val colsi = cols * i
      cforRange(0 until cols)(j => {
        val offsetJ = colsi + j
        v(offsetJ) += (vUpdatesI(j) - args.factorsReg * v(offsetJ)) * args.lr / sqrt(b(offsetJ) + 1e-07).toFloat
        b(offsetJ) += vUpdatesI(j) * vUpdatesI(j)
      })

      updatesArrayPool.putClear(vUpdates(i))
      vUpdates(i) = null
    })

    updateArraysArrayPool.put(vUpdates)
    indicesArrayPool.putClearUntil(vIndices, vIndex)
  }
}