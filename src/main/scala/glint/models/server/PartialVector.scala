package glint.models.server

import akka.actor.{Actor, ActorLogging}
import akka.routing.Routee
import glint.messages.server.request.PushSave
import glint.serialization.SerializableHadoopConfiguration
import glint.util.hdfs
import spire.algebra.{Order, Semiring}
import spire.implicits._

import scala.collection.immutable
import scala.reflect.ClassTag

private[glint] trait PartialVectorLogic[@specialized V] {

  implicit val semiring: Semiring[V]
  implicit val order: Order[V]
  implicit val classTag: ClassTag[V]

  val partitionId: Int

  var data: Array[V]

  /**
   * Updates the data of this partial model by aggregating given keys and values into it
   *
   * @param keys The keys
   * @param values The values
   */
  def update(keys: Array[Int], values: Array[V]): Boolean = {
    var i = 0
    while (i < keys.length) {
      data(keys(i)) += values(i)
      i += 1
    }
    true
  }

  /**
   * Gets the data of this partial model
   *
   * @param keys The keys
   * @return The values
   */
  def get(keys: Array[Int]): Array[V] = {
    var i = 0
    val a = new Array[V](keys.length)
    while (i < keys.length) {
      a(i) = data(keys(i))
      i += 1
    }
    a
  }

  /**
   * Saves the data of the partial vector to HDFS
   *
   * @param hdfsPath The HDFS base path the partial vector data should be saved to
   * @param hadoopConfig The serializable Hadoop configuration to use for saving to HDFS
   */
  def save(hdfsPath: String, hadoopConfig: SerializableHadoopConfiguration): Unit = {
    hdfs.savePartitionData(hdfsPath, hadoopConfig.conf, partitionId, data)
  }
}

/**
  * A partial model representing a part of some vector
  *
  * @param partitionId The partition
  * @tparam V The type of value to store
  */
private[glint] abstract class PartialVector[@specialized V]
(val partitionId: Int,
 val size: Int,
 val hdfsPath: Option[String],
 val hadoopConfig: Option[SerializableHadoopConfiguration])
(implicit val semiring: Semiring[V], val order: Order[V], val classTag: ClassTag[V])
  extends Actor with ActorLogging with PartialVectorLogic[V] with PushLogic {

  /**
    * The data vector containing the elements
    */
  var data: Array[V]

  override def receive: Receive = {
    case push: PushSave =>
      save(push.path, push.hadoopConfig)
      updateFinished(push.id)
    case x =>
      handleLogic(x, sender)
  }

  /**
   * Gets a loaded data vector if there is a HDFS path and a Hadoop configuration
   * Otherwise initializes a new data vector
   *
   * @param initialize The function to initialize the data vector
   * @param pathPostfix The path postfix added when loading the data vector. Use the same postfix used for saving
   * @return The data vector
   */
  def loadOrInitialize(initialize: => Array[V], pathPostfix: String = "/glint/data/"): Array[V] = {
    if (hdfsPath.isDefined && hadoopConfig.isDefined) {
      hdfs.loadPartitionData(hdfsPath.get, hadoopConfig.get.conf, partitionId, pathPostfix)
    } else {
      initialize
    }
  }

  log.info(s"Constructed PartialVector[${implicitly[ClassTag[V]]}] of size $size (partition id: ${partitionId})")
}

/**
 * A routee actor which accesses the shared mutable state of a partial vector
 *
 * @param data: The shared mutable data matrix
 */
private[glint] abstract class PartialVectorRoutee[@specialized V: Semiring : Order : ClassTag]
(val partitionId: Int, val routeeId: Int, var data: Array[V])
(implicit val semiring: Semiring[V], val order: Order[V], val classTag: ClassTag[V])
  extends Actor with ActorLogging with PartialVectorLogic[V] with RouteePushLogic {

  override def receive: Receive = {
    case push: PushSave =>
      save(push.path, push.hadoopConfig)
      updateFinished(push.id)
    case x =>
      handleLogic(x, sender)
  }
}

/**
 * Routing logic for partial vector routee actors
 */
private[glint] class PartialVectorRoutingLogic extends PushRoutingLogic {
  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {
    message match {
      case push: PushSave => routees(push.id >> 16)
      case _ => super.select(message, routees)
    }
  }
}