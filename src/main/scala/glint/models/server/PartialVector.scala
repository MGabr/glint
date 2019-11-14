package glint.models.server

import akka.actor.{Actor, ActorLogging}
import glint.partitioning.Partition
import glint.serialization.SerializableHadoopConfiguration
import glint.util.hdfs
import spire.algebra.Semiring
import spire.implicits._

import scala.reflect.ClassTag

/**
  * A partial model representing a part of some vector
  *
  * @param partition The partition
  * @tparam V The type of value to store
  */
private[glint] abstract class PartialVector[@specialized V: Semiring : ClassTag]
(partition: Partition,
 val hdfsPath: Option[String],
 val hadoopConfig: Option[SerializableHadoopConfiguration]) extends Actor with ActorLogging with PushLogic {

  /**
    * The size of this partial vector
    */
  val size: Int = partition.size

  /**
    * The data vector containing the elements
    */
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
    hdfs.savePartitionData(hdfsPath, hadoopConfig.conf, partition.index, data)
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
      hdfs.loadPartitionData(hdfsPath.get, hadoopConfig.get.conf, partition.index, pathPostfix)
    } else {
      initialize
    }
  }

  log.info(s"Constructed PartialVector[${implicitly[ClassTag[V]]}] of size $size (partition id: ${partition.index})")

}
