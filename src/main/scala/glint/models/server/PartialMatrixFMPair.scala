package glint.models.server

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.dispatch.Dispatchers
import akka.routing.{Group, Routee, Router}
import com.github.fommil.netlib.F2jBLAS
import glint.messages.server.logic.RouteesList
import glint.messages.server.request._
import glint.messages.server.response.{ResponseDotProdFM, ResponsePullSumFM}
import glint.serialization.SerializableHadoopConfiguration
import glint.util.hdfs.FMPairMetadata
import glint.util.{FloatArrayPool, hdfs}
import glint.{FMPairArguments, Server}
import org.eclipse.collections.api.block.procedure.primitive.IntObjectProcedure
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap
import spire.implicits._

import scala.collection.immutable
import scala.math.sqrt
import scala.util.Random

trait PartialMatrixFMPairLogic {

  val partitionId: Int
  val cols: Int
  val args: FMPairArguments
  val numFeatures: Int
  val avgActiveFeatures: Int
  val trainable: Boolean

  var v: Array[Float]
  var b: Array[Float]

  @transient
  private lazy val blas = new F2jBLAS

  /** Array pools and maps to avoid garbage collection */
  private val sumsArrayPool = new FloatArrayPool(args.batchSize * cols)
  private val updatesArrayPool = new FloatArrayPool(cols)
  private val updatesMap = new IntObjectHashMap[Array[Float]](args.batchSize * avgActiveFeatures * 10)

  /** Caches to avoid duplicate transmission of indices and weights */
  private val cacheDotProd = new IntObjectHashMap[(Array[Array[Int]], Array[Array[Float]], Array[Float], Array[Array[Int]], Array[Array[Float]], Array[Float])]()
  private val cachePullSum = new IntObjectHashMap[(Array[Array[Int]], Array[Array[Float]])]()

  /**
   * Update procedure which updates the weights according to the supplied partial gradient updates.
   * Uses an Adagrad learning rate and frequency adaptive L2 regularization.
   *
   * Defined as variable to avoid the creation of an anonymous implementation on each method call
   */
  private val updateProcedure = new IntObjectProcedure[Array[Float]] {
    override def value(i: Int, vUpdatesI: Array[Float]): Unit = {
      val colsi = cols * i
      cforRange(0 until cols)(j => {
        val offsetJ = colsi + j
        v(offsetJ) += (vUpdatesI(j) - args.factorsReg * v(offsetJ)) * args.lr / sqrt(b(offsetJ) + 1e-07).toFloat
        b(offsetJ) += vUpdatesI(j) * vUpdatesI(j)
      })
      updatesArrayPool.putClear(vUpdatesI)
    }
  }

  /**
   * Returns the partial dot products
   *
   * @param iUser The user feature indices
   * @param wUser The user feature weights
   * @param iItem The item feature indices
   * @param wItem The item feature weights
   * @param cacheKey The key to cache the indices and weights
   * @param cache Whether the indices and weights should be cached
   */
  def dotprod(iUser: Array[Array[Int]],
              wUser: Array[Array[Float]],
              iItem: Array[Array[Int]],
              wItem: Array[Array[Float]],
              cacheKey: Int,
              cache: Boolean): Array[Float] = {

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

    cforRange(0 until iUser.length)(i => {
      val iu = iUser(i); val wu = wUser(i)
      val ii = iItem(i); val wi = wItem(i)
      val sOffset = i * cols
      val gb = g(i)

      cforRange(0 until iu.length)(j => {
        blas.saxpy(cols, gb * wu(j), sItem, sOffset, 1, updatesMap.getIfAbsentPut(iu(j), updatesArrayPool.get()), 0, 1)
      })

      cforRange(0 until ii.length)(j => {
        blas.saxpy(cols, gb * wi(j), sUser, sOffset, 1, updatesMap.getIfAbsentPut(ii(j), updatesArrayPool.get()), 0, 1)
      })
    })

    updatesMap.forEachKeyValue(updateProcedure)

    sumsArrayPool.putClear(sUser)
    sumsArrayPool.putClear(sItem)
    updatesMap.clear()
  }

  /**
   * Returns the weighted partial sums of the feature indices
   *
   * @param indices The feature indices
   * @param weights The feature weights
   * @param cacheKey The key to cache the indices and weights
   * @param cache Whether the indices, weights and sums should be cached
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

    cforRange(0 until indices.length)(i => {
      val ii = indices(i); val wi = weights(i)
      val gOffset = i * cols
      cforRange(0 until ii.length)(j => {
        blas.saxpy(cols, wi(j), g, gOffset, 1, updatesMap.getIfAbsentPut(ii(j), updatesArrayPool.get()), 0, 1)
      })
    })

    updatesMap.forEachKeyValue(updateProcedure)
    updatesMap.clear()
  }

  def save(hdfsPath: String, hadoopConfig: SerializableHadoopConfiguration, saveTrainable: Boolean): Unit = {
    // the partial matrix holding the first partition also saves metadata
    if (partitionId == 0) {
      val meta = FMPairMetadata(args, numFeatures, avgActiveFeatures, trainable && saveTrainable)
      hdfs.saveFMPairMetadata(hdfsPath, hadoopConfig.conf, meta)
    }

    hdfs.savePartitionData(hdfsPath, hadoopConfig.conf, partitionId, v, pathPostfix = "/glint/data/v/")
    if (trainable && saveTrainable) {
      hdfs.savePartitionData(hdfsPath, hadoopConfig.conf, partitionId, b, pathPostfix = "/glint/data/b/")
    }
  }
}

/**
 * A partial matrix holding floats and supporting specific messages for efficient distributed FMPair training
 */
private[glint] class PartialMatrixFMPair(partitionId: Int,
                                         cols: Int,
                                         hdfsPath: Option[String],
                                         hadoopConfig: Option[SerializableHadoopConfiguration],
                                         val args: FMPairArguments,
                                         val numFeatures: Int,
                                         val avgActiveFeatures: Int,
                                         val trainable: Boolean)
  extends PartialMatrixFloat(partitionId, numFeatures, cols, hdfsPath, hadoopConfig)
    with PartialMatrixFMPairLogic {

  /** The latent factors matrix */
  var v: Array[Float] = _

  /** The Adagrad buffers of the latent factors matrix */
  var b: Array[Float] = _

  /** The routees which access the above matrices as shared mutable state */
  var routees: Array[ActorRef] = Array()

  override def preStart(): Unit = {
    val random = new Random(partitionId)
    v = loadOrInitialize(Array.fill(rows * cols)(random.nextFloat() * 0.2f - 0.1f), pathPostfix = "/glint/data/v/")
    data = v

    if (trainable) {
      b = loadOrInitialize(Array.fill(rows * cols)(0.1f), pathPostfix = "/glint/data/b/")
    }

    routees = (0 until Server.cores).toArray.map(routeeId =>
      context.system.actorOf(Props(classOf[PartialMatrixFMPairRoutee], routeeId, partitionId, rows, cols,
        args, numFeatures, avgActiveFeatures, trainable, v, b).withDispatcher("pinned-dispatcher")))
  }

  private def fmpairReceive: Receive = {
    case pull: PullDotProdFM =>
      val id = nextId()
      sender ! ResponseDotProdFM(dotprod(pull.iUser, pull.wUser, pull.iItem, pull.wItem, id, pull.cache), id)
    case pull: PullSumFM =>
      val id = nextId()
      sender ! ResponsePullSumFM(pullSum(pull.indices, pull.weights, id, pull.cache), id)
    case push: PushAdjustFM =>
      adjust(push.g, push.id)
      updateFinished(push.id)
    case push: PushSumFM =>
      pushSum(push.g, push.id)
      updateFinished(push.id)
    case push: PushSaveTrainable =>
      save(push.path, push.hadoopConfig, push.trainable)
      updateFinished(push.id)
    case RouteesList() =>
      sender ! routees
  }

  override def receive: Receive = fmpairReceive.orElse(super.receive)

  override def save(hdfsPath: String, hadoopConfig: SerializableHadoopConfiguration): Unit = {
    save(hdfsPath, hadoopConfig, true)
  }
}

/**
 * A routee actor which accesses the shared mutable state of a partial FMPair matrix
 *
 * @param v: The shared mutable partial latent factors matrix
 * @param b: The shared mutable Adagrad buffers of the partial latent factors matrix
 */
class PartialMatrixFMPairRoutee(routeeId: Int,
                                partitionId: Int,
                                rows: Int,
                                cols: Int,
                                val args: FMPairArguments,
                                val numFeatures: Int,
                                val avgActiveFeatures: Int,
                                val trainable: Boolean,
                                var v: Array[Float],
                                var b: Array[Float])
  extends PartialMatrixFloatRoutee(routeeId, partitionId, rows, cols, v) with PartialMatrixFMPairLogic {

  private def fmpairReceive: Receive = {
    case pull: PullDotProdFM =>
      val id = nextId()
      sender ! ResponseDotProdFM(dotprod(pull.iUser, pull.wUser, pull.iItem, pull.wItem, id, pull.cache), id)
    case pull: PullSumFM =>
      val id = nextId()
      sender ! ResponsePullSumFM(pullSum(pull.indices, pull.weights, id, pull.cache), id)
    case push: PushAdjustFM =>
      adjust(push.g, push.id)
      updateFinished(push.id)
    case push: PushSumFM =>
      pushSum(push.g, push.id)
      updateFinished(push.id)
    case push: PushSaveTrainable =>
      save(push.path, push.hadoopConfig, push.trainable)
      updateFinished(push.id)
  }

  override def receive: Receive = fmpairReceive.orElse(super.receive)

  override def save(hdfsPath: String, hadoopConfig: SerializableHadoopConfiguration): Unit = {
    save(hdfsPath, hadoopConfig, true)
  }
}

/**
 * Routing logic for partial FMPair matrix routee actors
 */
private[glint] class PartialMatrixFMPairRoutingLogic extends PartialMatrixFloatRoutingLogic {

  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {
    message match {
      case push: PushAdjustFM => routees(push.id >> 16)
      case push: PushSumFM => routees(push.id >> 16)
      case push: PushSaveTrainable => routees(push.id >> 16)
      case _ => super.select(message, routees)
    }
  }
}

/**
 * A group router for partial FMPair matrix routee actors
 *
 * @param paths The paths of the routees
 */
private[glint] case class PartialMatrixFMPairGroup(paths: immutable.Iterable[String]) extends Group {

  override def paths(system: ActorSystem): immutable.Iterable[String] = paths

  override def createRouter(system: ActorSystem): Router = new Router(new PartialMatrixFMPairRoutingLogic)

  override def routerDispatcher: String = Dispatchers.DefaultDispatcherId
}