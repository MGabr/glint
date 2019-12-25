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


private[glint] trait PartialMatrixLogic[@specialized V] {

  implicit def semiring: Semiring[V]
  implicit def order: Order[V]
  implicit def classTag: ClassTag[V]

  val partitionId: Int
  val rows: Int
  val cols: Int

  var data: Array[V]

  /**
   * Returns rows from the data matrix
   *
   * @param rows The row indices
   */
  def getRows(rows: Array[Int]): Array[Array[V]] = {
    var i = 0
    val a = new Array[Array[V]](rows.length)
    while (i < rows.length) {
      a(i) = data.slice(rows(i) * cols, rows(i) * cols + cols)
      i += 1
    }
    a
  }

  /**
   * Returns values from the data matrix
   *
   * @param rows The row indices
   * @param cols The column indices
   */
  def get(rows: Array[Int], cols: Array[Int]): Array[V] = {
    var i = 0
    val a = new Array[V](rows.length)
    while (i < rows.length) {
      a(i) = data(rows(i) * this.cols + cols(i))
      i += 1
    }
    a
  }

  /**
   * Updates the data of this partial model by aggregating given keys and values into it
   *
   * @param rows The rows
   * @param cols The cols
   * @param values The values
   */
  def update(rows: Array[Int], cols: Array[Int], values: Array[V]): Unit = {
    var i = 0
    while (i < rows.length) {
      data(rows(i) * this.cols + cols(i)) += values(i)
      i += 1
    }
  }

  /**
   * Saves the data of the partial matrix to HDFS
   *
   * @param hdfsPath The HDFS base path the partial matrix data should be saved to
   * @param hadoopConfig The serializable Hadoop configuration to use for saving to HDFS
   */
  def save(hdfsPath: String, hadoopConfig: SerializableHadoopConfiguration): Unit = {
    hdfs.savePartitionData(hdfsPath, hadoopConfig.conf, partitionId, data)
  }
}


/**
  * A partial model representing a part of some matrix
  *
  * @param partitionId The id of the partition of data this partial matrix represents
  * @param rows The number of rows
  * @param cols The number of columns
  * @param hdfsPath The HDFS base path from which the partial matrix' initial data should be loaded from
  * @param hadoopConfig The serializable Hadoop configuration to use for loading the initial data from HDFS
  * @tparam V The type of value to store
  */
private[glint] abstract class PartialMatrix[@specialized V]
(val partitionId: Int,
 val rows: Int,
 val cols: Int,
 val hdfsPath: Option[String],
 val hadoopConfig: Option[SerializableHadoopConfiguration])
(implicit val semiring: Semiring[V], val order: Order[V], val classTag: ClassTag[V])
  extends Actor with ActorLogging with PartialMatrixLogic[V] with PushLogic {

  /** The data matrix containing the elements */
  var data: Array[V]

  override def receive: Receive = {
    case push: PushSave =>
      save(push.path, push.hadoopConfig)
      updateFinished(push.id)
    case x =>
      handleLogic(x, sender)
  }

  /**
    * Returns a loaded data matrix if a HDFS path and a Hadoop configuration are defined.
    * Otherwise returns a new initialized data matrix
    *
    * @param initialize The function to initialize the data matrix
    * @param pathPostfix The path postfix added when loading the data matrix. Use the same postfix used for saving
    */
  def loadOrInitialize(initialize: => Array[V], pathPostfix: String = "/glint/data/"): Array[V] = {
    if (hdfsPath.isDefined && hadoopConfig.isDefined) {
      hdfs.loadPartitionData(hdfsPath.get, hadoopConfig.get.conf, partitionId, pathPostfix)
    } else {
      initialize
    }
  }

  log.info(s"Constructed PartialMatrix[${implicitly[ClassTag[V]]}] with $rows rows and $cols columns (partition id: ${partitionId})")
}


/**
 * A routee actor which accesses the shared mutable state of a partial matrix
 *
 * @param data: The shared mutable data matrix
 */
private[glint] abstract class PartialMatrixRoutee[@specialized V]
(val routeeId: Int, val partitionId: Int, val rows: Int, val cols: Int, var data: Array[V])
(implicit val semiring: Semiring[V], val order: Order[V], val classTag: ClassTag[V])
  extends Actor with ActorLogging with PartialMatrixLogic[V] with RouteePushLogic {

  override def receive: Receive = {
    case push: PushSave =>
      save(push.path, push.hadoopConfig)
      updateFinished(push.id)
    case x =>
      handleLogic(x, sender)
  }
}

/**
 * Routing logic for partial matrix routee actors
 */
private[glint] class PartialMatrixRoutingLogic extends PushRoutingLogic {
  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {
    message match {
      case push: PushSave => routees(push.id >> 16)
      case _ => super.select(message, routees)
    }
  }
}