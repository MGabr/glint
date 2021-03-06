package glint.models.server

import akka.pattern.pipe
import com.github.fommil.netlib.F2jBLAS
import glint.FMPairArguments
import glint.messages.server.request._
import glint.messages.server.response.{ResponseDotProdFM, ResponseFloat, ResponsePullSumFM, ResponseRowsFloat}
import glint.models.server.aggregate.Aggregate
import glint.partitioning.Partition
import glint.serialization.SerializableHadoopConfiguration
import glint.util.hdfs.FMPairMetadata
import glint.util.{FloatArrayPool, hdfs}
import org.eclipse.collections.api.block.procedure.primitive.IntObjectProcedure
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap
import spire.implicits.cforRange

import scala.concurrent.Future
import scala.math.pow
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
  * @param numConcurrentBatches The number of concurrent batches to use for AdaBatch reconditioning,
  *                             this will usually be the number of executors communicating with the vector concurrently
  * @param loadFeatureProbsOnly Whether only the feature probabilities or the full data should be loaded from HDFS
  * @param trainable Whether the matrix is trainable, requiring more data (AdaBatch buffers)
  */
private[glint] class PartialMatrixFMPair(partition: Partition,
                                         aggregate: Aggregate,
                                         hdfsPath: Option[String],
                                         hadoopConfig: Option[SerializableHadoopConfiguration],
                                         val args: FMPairArguments,
                                         val numFeatures: Int,
                                         val avgActiveFeatures: Int,
                                         val numConcurrentBatches: Int,
                                         val loadFeatureProbsOnly: Boolean,
                                         val trainable: Boolean)
  extends PartialMatrixFloat(partition, numFeatures, partition.size, aggregate, hdfsPath, hadoopConfig) {

  @transient
  private lazy val blas = new F2jBLAS

  /**
    * The latent factors matrix
    */
  var v: Array[Float] = _

  /**
   * Precomputed AdaBatch reconditioning weight for each feature / matrix column.
   * See "AdaBatch: Efficient Gradient Aggregation Rules for Sequential and Parallel Stochastic Gradient Methods"
   */
  var cb: Array[Float] = _

  var featureProbs: Array[Float] = _

  override def loadOrInitialize(initialize: => Array[Float], pathPostfix: String): Array[Float] = {
    if (loadFeatureProbsOnly) initialize else super.loadOrInitialize(initialize, pathPostfix)
  }

  override def preStart(): Unit = {
    val random = new Random(partition.index)
    v = loadOrInitialize(Array.fill(rows * cols)(random.nextFloat() * 0.2f - 0.1f), pathPostfix = "/glint/data/v/")
    data = v

    if (trainable) {
      // store feature probabilites to allow saving it to HDFS as metadata later
      val featureProbs = hdfs.loadFMPairMetadata(hdfsPath.get, hadoopConfig.get.get()).featureProbs
      if (partition.index == 0) {
        this.featureProbs = featureProbs
      }
      cb = adabatchCB(featureProbs)
    }
  }

  /**
   * Precomputes AdaBatch reconditioning weights from given probabilities.
   * The learning rate and the division by the batch size is included in the reconditioning weights
   *
   * @param featureProbs The feature probabilities
   */
  private def adabatchCB(featureProbs: Array[Float]): Array[Float] = {
    val b = (args.batchSize * numConcurrentBatches).toDouble
    featureProbs.map(p => args.lr * (((1 - pow(1 - p, b)) / p) / b).toFloat)
  }


  /**
   * Thread local array pools and map to avoid garbage collection
   */
  private val threadLocalSumsArrayPool = new ThreadLocal[FloatArrayPool] {
    override def initialValue(): FloatArrayPool = new FloatArrayPool(args.batchSize * cols)
  }

  private val threadLocalUpdatesArrayPool = new ThreadLocal[FloatArrayPool] {
    override def initialValue(): FloatArrayPool = new FloatArrayPool(cols)
  }

  private val threadLocalUpdatesMap = new ThreadLocal[IntObjectHashMap[Array[Float]]] {
    override def initialValue() = new IntObjectHashMap[Array[Float]](args.batchSize * avgActiveFeatures * 10)
  }


  /**
   * Update procedure which updates the weights according to the supplied partial gradient updates.
   * Uses an an adaptive AdaBatch learning rate for mini-batches with sparse gradients.
   *
   * This is defined as thread local implementation to avoid the creation of an anonymous implementation
   * on each method call.
   */
  private val threadLocalUpdateProcedure = new ThreadLocal[IntObjectProcedure[Array[Float]]] {
    override def initialValue(): IntObjectProcedure[Array[Float]] = new IntObjectProcedure[Array[Float]] {

      private val updatesArrayPool = threadLocalUpdatesArrayPool.get()

      override def value(i: Int, vUpdatesI: Array[Float]): Unit = {
        blas.saxpy(cols, cb(i), vUpdatesI, 0, 1, v, cols * i, 1)
        updatesArrayPool.putClear(vUpdatesI)
      }
    }
  }


  /**
   * Caches to avoid duplicate transmission of indices and weights
   */
  private val cacheDotProd = new ConcurrentHashMap[
    Int,
    (Array[Array[Int]], Array[Array[Float]], Array[Float], Array[Array[Int]], Array[Array[Float]], Array[Float])]()

  private val cachePullSum = new ConcurrentHashMap[Int, (Array[Array[Int]], Array[Array[Float]])]()

  private var lastCacheKey = 0


  /**
    * Receives and handles incoming messages
    *
    * The specific messages for efficient distributed FM-Pair training (dotprod / pullSum and adjust / pushSum) are
    * executed asynchronously and then send their result back to the sender. This means that we lose the Akka
    * concurrency guarantees and the methods access the actors state as shared mutable state without any
    * synchronization and locking. These methods can therefore overwrite each others gradient updates.
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
        true
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
        true
      } pipeTo sender()
    case x =>
      handleLogic(x, sender)
  }

  def save(hdfsPath: String, hadoopConfig: SerializableHadoopConfiguration, saveTrainable: Boolean): Unit = {

    // the partial matrix holding the first partition also saves metadata
    if (partition.index == 0) {
      val meta = FMPairMetadata(args, featureProbs, avgActiveFeatures, trainable && saveTrainable)
      hdfs.saveFMPairMetadata(hdfsPath, hadoopConfig.conf, meta)
    }

    hdfs.savePartitionData(hdfsPath, hadoopConfig.conf, partition.index, v, pathPostfix = "/glint/data/v/")
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

    // for asynchronous exactly-once delivery with PullFSM
    val cached = cacheDotProd.remove(cacheKey)
    if (cached == null) {
      return
    }
    val (iUser, wUser, sUser, iItem, wItem, sItem) = cached

    // use pools to prevent garbage collection
    val sumsArrayPool = threadLocalSumsArrayPool.get()
    val updatesArrayPool = threadLocalUpdatesArrayPool.get()
    val vUpdates = threadLocalUpdatesMap.get()

    cforRange(0 until iUser.length)(i => {
      val iu = iUser(i); val wu = wUser(i)
      val ii = iItem(i); val wi = wItem(i)
      val sOffset = i * cols
      val gb = g(i)

      cforRange(0 until iu.length)(j => {
        blas.saxpy(cols, gb * wu(j), sItem, sOffset, 1, vUpdates.getIfAbsentPut(iu(j), updatesArrayPool.getFunction), 0, 1)
        blas.saxpy(cols, -args.factorsReg, v, iu(j) * cols, 1, vUpdates.getIfAbsentPut(iu(j), updatesArrayPool.getFunction), 0, 1)
      })

      cforRange(0 until ii.length)(j => {
        blas.saxpy(cols, gb * wi(j), sUser, sOffset, 1, vUpdates.getIfAbsentPut(ii(j), updatesArrayPool.getFunction), 0, 1)
        blas.saxpy(cols, -args.factorsReg, v, ii(j) * cols, 1, vUpdates.getIfAbsentPut(ii(j), updatesArrayPool.getFunction), 0, 1)
      })
    })

    vUpdates.forEachKeyValue(threadLocalUpdateProcedure.get())

    sumsArrayPool.putClear(sUser)
    sumsArrayPool.putClear(sItem)
    vUpdates.clear()
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

    // for asynchronous exactly-once delivery with PullFSM
    val cached = cachePullSum.remove(cacheKey)
    if (cached == null) {
      return
    }
    val (indices, weights) = cached

    // use pools to prevent garbage collection
    val updatesArrayPool = threadLocalUpdatesArrayPool.get()
    val vUpdates = threadLocalUpdatesMap.get()

    cforRange(0 until indices.length)(i => {
      val ii = indices(i); val wi = weights(i)
      val gOffset = i * cols
      cforRange(0 until ii.length)(j => {
        blas.saxpy(cols, wi(j), g, gOffset, 1, vUpdates.getIfAbsentPut(ii(j), updatesArrayPool.getFunction), 0, 1)
        blas.saxpy(cols, -args.factorsReg, v, ii(j) * cols, 1, vUpdates.getIfAbsentPut(ii(j), updatesArrayPool.getFunction), 0, 1)
      })
    })

    vUpdates.forEachKeyValue(threadLocalUpdateProcedure.get())
    vUpdates.clear()
  }
}
