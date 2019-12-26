package glint.models.server

import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.dispatch.Dispatchers
import akka.routing.{Group, Routee, Router}
import glint.messages.server.logic.RouteesList
import glint.messages.server.request._
import glint.messages.server.response.ResponsePullSumFM
import glint.serialization.SerializableHadoopConfiguration
import glint.util.hdfs
import glint.util.hdfs.FMPairMetadata
import glint.{FMPairArguments, Server}
import org.eclipse.collections.api.block.procedure.primitive.IntFloatProcedure
import org.eclipse.collections.impl.map.mutable.primitive.IntFloatHashMap
import spire.implicits.cforRange

import scala.collection.immutable
import scala.math.sqrt
import scala.util.Random

trait PartialVectorFMPairLogic {

  val partitionId: Int
  val args: FMPairArguments
  val numFeatures: Int
  val avgActiveFeatures: Int
  val trainable: Boolean

  var w: Array[Float]
  var b: Array[Float]

  /** Map to avoid garbage collection */
  private val updatesMap = new IntFloatHashMap(args.batchSize * avgActiveFeatures)

  /**
   * Update procedure which updates the weights according to the supplied partial gradient updates.
   * Uses an Adagrad learning rate and frequency adaptive L2 regularization.
   *
   * This is defined as thread local implementation to avoid the creation of an anonymous implementation
   * on each method call.
   */
  private val updateProcedure = new IntFloatProcedure {
    override def value(i: Int, wUpdateI: Float): Unit = {
      w(i) += (wUpdateI - args.linearReg * w(i)) * args.lr / sqrt(b(i) + 1e-07).toFloat
      b(i) += wUpdateI * wUpdateI
    }
  }

  /** Cache to avoid duplicate transmission of indices and weights */
  private val cachePullSum = new ConcurrentHashMap[Int, (Array[Array[Int]], Array[Array[Float]])]()

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
    val (indices, weights) = cachePullSum.get(cacheKey)
    cachePullSum.remove(cacheKey)

    cforRange(0 until indices.length)(i => {
      val ii = indices(i); val wi = weights(i); val gi = g(i)
      cforRange(0 until ii.length)(j => {
        updatesMap.addToValue(ii(j), gi * wi(j))
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

    hdfs.savePartitionData(hdfsPath, hadoopConfig.conf, partitionId, w, pathPostfix = "/glint/data/w/")
    if (trainable && saveTrainable) {
      hdfs.savePartitionData(hdfsPath, hadoopConfig.conf, partitionId, b, pathPostfix = "/glint/data/wb/")
    }
  }
}

/**
 * A partial vector holding floats and supporting specific messages for efficient distributed FMPair training
 */
private[glint] class PartialVectorFMPair(partitionId: Int,
                                         size: Int,
                                         hdfsPath: Option[String],
                                         hadoopConfig: Option[SerializableHadoopConfiguration],
                                         val args: FMPairArguments,
                                         val numFeatures: Int,
                                         val avgActiveFeatures: Int,
                                         val trainable: Boolean)
  extends PartialVectorFloat(partitionId, size, hdfsPath,  hadoopConfig) with PartialVectorFMPairLogic {

  /** The linear weights vector */
  var w: Array[Float] = _

  /** The Adagrad buffers of the linear weights vector */
  var b: Array[Float] = _

  /** The routees which access the above vectors as shared mutable state */
  var routees: Array[ActorRef] = Array()

  override def preStart(): Unit = {
    val random = new Random(partitionId)
    w = loadOrInitialize(Array.fill(size)(random.nextFloat() * 0.2f - 0.1f), pathPostfix = "/glint/data/w/")
    data = w

    if (trainable) {
      b = loadOrInitialize(Array.fill(size)(0.1f), pathPostfix = "/glint/data/wb/")
    }

    routees = (0 until Server.cores).toArray.map(routeeId =>
      context.system.actorOf(Props(classOf[PartialVectorFMPairRoutee], partitionId, routeeId,
        args, numFeatures, avgActiveFeatures, trainable, w, b).withDispatcher("pinned-dispatcher")))
  }

  private def fmpairReceive: Receive = {
    case pull: PullSumFM =>
      val id = nextId()
      sender ! ResponsePullSumFM(pullSum(pull.indices, pull.weights, id, pull.cache), id)
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
 * A routee actor which accesses the shared mutable state of a partial FMPair vector
 *
 * @param w: The shared mutable partial linear weights
 * @param b: The shared mutable Adagrad buffers of the partial linear weights
 */
private[glint] class PartialVectorFMPairRoutee(partitionId: Int,
                                               routeeId: Int,
                                               val args: FMPairArguments,
                                               val numFeatures: Int,
                                               val avgActiveFeatures: Int,
                                               val trainable: Boolean,
                                               var w: Array[Float],
                                               var b: Array[Float])
  extends PartialVectorFloatRoutee(partitionId, routeeId, w) with PartialVectorFMPairLogic {

  private def fmpairReceive: Receive = {
    case pull: PullSumFM =>
      val id = nextId()
      sender ! ResponsePullSumFM(pullSum(pull.indices, pull.weights, id, pull.cache), id)
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
 * Routing logic for partial FMPair vector routee actors
 */
private[glint] class PartialVectorFMPairRoutingLogic extends PartialVectorFloatRoutingLogic {
  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {
    message match {
      case push: PushSumFM => routees(push.id >> 16)
      case push: PushSaveTrainable => routees(push.id >> 16)
      case _ => super.select(message, routees)
    }
  }
}

/**
 * A group router for partial FMPair vector routee actors
 *
 * @param paths The paths of the routees
 */
private[glint] case class PartialVectorFMPairGroup(paths: immutable.Iterable[String]) extends Group {

  override def paths(system: ActorSystem): immutable.Iterable[String] = paths

  override def createRouter(system: ActorSystem): Router = new Router(new PartialVectorFMPairRoutingLogic)

  override def routerDispatcher: String = Dispatchers.DefaultDispatcherId
}