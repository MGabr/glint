package glint.partitioning.range

import glint.partitioning.Partition
import glint.partitioning.by.PartitionBy.PartitionBy
import glint.partitioning.by.{ColPartition, PartitionBy, RowPartition}

/**
  * A range partition
  */
private[partitioning] class RangePartition(index: Int, val start: Long, val end: Long) extends Partition(index) {

  /**
    * Checks whether given global key falls within this partition
    *
    * @param key The key
    * @return True if the global key falls within this partition, false otherwise
    */
  @inline
  override def contains(key: Long): Boolean = key >= start && key < end

  /**
    * Computes the size of this partition
    *
    * @return The size of this partition
    */
  override def size: Int = (end - start).toInt

  /**
    * Converts given global row key to a continuous local array index [0, 1, ...]
    *
    * @param key The global row key
    * @return The local index
    */
  @inline
  override def globalRowToLocal(key: Long): Int = (key - start).toInt

  /**
    * Converts given global column key to a continuous local array index [0, 1, ...]
    *
    * @param key The global column key
    * @return The local index
    */
  @inline
  override def globalColToLocal(key: Long): Int = (key - start).toInt

  /**
    * Converts given row index in a continuous local array [0, 1, ...] to a global key
    *
    * @param index The local row index
    * @return The global key
    */
  @inline
  override def localRowToGlobal(index: Int): Long = {
    index + start
  }

  /**
    * Converts given column index in a continuous local array [0, 1, ...] to a global key
    *
    * @param index The local column index
    * @return The global key
    */
  @inline
  override def localColToGlobal(index: Int): Long = {
    index + start
  }
}

object RangePartition {

  def apply(index: Int, start: Long, end: Long, by: PartitionBy): RangePartition = {
    if (by == PartitionBy.ROW) {
      new RangePartition(index, start, end) with RowPartition
    } else {
      new RangePartition(index, start, end) with ColPartition
    }
  }
}
