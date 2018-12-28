package glint.models.server

import akka.actor.{Actor, ActorLogging}
import glint.models.server.aggregate.Aggregate
import spire.algebra.{Order, Semiring}
import spire.implicits._
import glint.partitioning.Partition

import scala.reflect.ClassTag

/**
  * A partial model representing a part of some matrix
  *
  * @param partition The partition of data this partial matrix represents
  * @param rows The number of rows
  * @param cols The number of columns
  * @param aggregate The type of aggregation to apply
  * @tparam V The type of value to store
  */
private[glint] abstract class PartialMatrix[@specialized V: Semiring : Order : ClassTag](val partition: Partition,
                                                                                         val rows: Int,
                                                                                         val cols: Int,
                                                                                         val aggregate: Aggregate)
  extends Actor with ActorLogging with PushLogic {

  /**
    * The data matrix containing the elements
    */
  val data: Array[V]

  /**
    * Gets rows from the data matrix
    *
    * @param rows The row indices
    * @return A sequence of values
    */
  def getRows(rows: Array[Long]): Array[Array[V]] = {
    var i = 0
    val a = new Array[Array[V]](rows.length)
    while (i < rows.length) {
      val row = partition.globalRowToLocal(rows(i))
      a(i) = data.slice(row * cols, row * cols + cols)
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
  def get(rows: Array[Long], cols: Array[Long]): Array[V] = {
    var i = 0
    val a = new Array[V](rows.length)
    while (i < rows.length) {
      val row = partition.globalRowToLocal(rows(i))
      val col = partition.globalColToLocal(cols(i))
      a(i) = data(row * this.cols + col)
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
  def update(rows: Array[Long], cols: Array[Long], values: Array[V]): Boolean = {
    var i = 0
    while (i < rows.length) {
      val row = partition.globalRowToLocal(rows(i))
      val col = partition.globalColToLocal(cols(i))
      data(row * this.cols + col) = aggregate.aggregate[V](data(row * this.cols + col), values(i))
      i += 1
    }
    true
  }

  log.info(s"Constructed PartialMatrix[${implicitly[ClassTag[V]]}] with $rows rows and $cols columns (partition id: ${partition.index})")

}
