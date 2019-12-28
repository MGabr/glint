package glint.models.server

import akka.actor.{Actor, ActorLogging}
import glint.models.server.aggregate.Aggregate
import glint.partitioning.Partition
import glint.serialization.SerializableHadoopConfiguration
import glint.util.hdfs
import spire.algebra.{Order, Semiring}

import scala.reflect.ClassTag

/**
  * A partial model representing a part of some matrix
  *
  * @param partition The partition of data this partial matrix represents
  * @param rows The number of rows
  * @param cols The number of columns
  * @param aggregate The type of aggregation to apply
  * @param hdfsPath The HDFS base path from which the partial matrix' initial data should be loaded from
  * @param hadoopConfig The serializable Hadoop configuration to use for loading the initial data from HDFS
  * @tparam V The type of value to store
  */
private[glint] abstract class PartialMatrix[@specialized V: Semiring : Order : ClassTag]
(val partition: Partition,
 val rows: Int,
 val cols: Int,
 val aggregate: Aggregate,
 val hdfsPath: Option[String],
 val hadoopConfig: Option[SerializableHadoopConfiguration]) extends Actor with ActorLogging with PushLogic {

  /**
    * The data matrix containing the elements
    */
  var data: Array[V]

  /**
    * The execution context in which asynchronously handled message can be executed.
    * This leads to all partial matrices on a server sharing the thread pool in a fair way
    */
  implicit val ec = context.dispatcher

  /**
    * Gets rows from the data matrix
    *
    * @param rows The row indices
    * @return A sequence of values
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
    * Gets values from the data matrix
    *
    * @param rows The row indices
    * @param cols The column indices
    * @return A sequence of values
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
      data(rows(i) * this.cols + cols(i)) = aggregate.aggregate[V](data(rows(i) * this.cols + cols(i)), values(i))
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
    hdfs.savePartitionData(hdfsPath, hadoopConfig.conf, partition.index, data)
  }

  /**
    * Gets a loaded data matrix if there is a HDFS path and a Hadoop configuration
    * Otherwise initializes a new data matrix
    *
    * @param initialize The function to initialize the data matrix
    * @param pathPostfix The path postfix added when loading the data matrix. Use the same postfix used for saving
    * @return The data matrix
    */
  def loadOrInitialize(initialize: => Array[V], pathPostfix: String = "/glint/data/"): Array[V] = {
    if (hdfsPath.isDefined && hadoopConfig.isDefined) {
      hdfs.loadPartitionData(hdfsPath.get, hadoopConfig.get.conf, partition.index, pathPostfix)
    } else {
      initialize
    }
  }

  log.info(s"Constructed PartialMatrix[${implicitly[ClassTag[V]]}] with $rows rows and $cols columns (partition id: ${partition.index})")

}
