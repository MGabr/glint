package glint.partitioning.cyclic

import glint.partitioning.by.PartitionBy.PartitionBy
import glint.partitioning.by.{ColPartition, PartitionBy, RowPartition}
import glint.partitioning.Partition

/**
  * A cyclic partition
  *
  * @param index The index of this partition
  * @param numberOfPartitions The total number of partitions
  * @param numberOfKeys The total number of keys
  */
private[partitioning] class CyclicPartition(index: Int,
                                            val numberOfPartitions: Int,
                                            numberOfKeys: Long) extends Partition(index) {

  /**
    * Checks whether given global key falls within this partition
    *
    * @param key The key
    * @return True if the global key falls within this partition, false otherwise
    */
  @inline
  override def contains(key: Long): Boolean = {
    (key % numberOfPartitions).toInt == index
  }

  /**
    * Computes the size of this partition
    *
    * @return The size of this partition
    */
  override def size: Int = {
    var i = 1
    while (!contains(numberOfKeys - i)) {
      i += 1
    }
    globalKeyToLocal(numberOfKeys - i) + 1
  }

  private def globalKeyToLocal(key: Long): Int = {
    (globalRowToLocal(key) + globalColToLocal(key) - key).toInt
  }

  /**
    * Converts given global key to a continuous local array index [0, 1, ...]
    *
    * @param key The global key
    * @return The local index
    */
  @inline
  override def globalRowToLocal(key: Long): Int = {
    ((key - index) / numberOfPartitions).toInt
  }

  /**
    * Converts given global key to a continuous local array index [0, 1, ...]
    *
    * @param key The global key
    * @return The local index
    */
  @inline
  override def globalColToLocal(key: Long): Int = {
    ((key - index) / numberOfPartitions).toInt
  }

  /**
    * Converts given row index in a continuous local array [0, 1, ...] to a global key
    *
    * @param index The local row index
    * @return The global key
    */
  @inline
  override def localRowToGlobal(index: Int): Long = {
    index * numberOfPartitions + this.index
  }

  /**
    * Converts given column index in a continuous local array [0, 1, ...] to a global key
    *
    * @param index The local column index
    * @return The global key
    */
  @inline
  override def localColToGlobal(index: Int): Long = {
    index * numberOfPartitions + this.index
  }
}

object CyclicPartition {

  def apply(index: Int, numberOfPartitions: Int, numberOfKeys: Long, by: PartitionBy): CyclicPartition = {
    if (by == PartitionBy.ROW) {
      new CyclicPartition(index, numberOfPartitions, numberOfKeys) with RowPartition
    } else {
      new CyclicPartition(index, numberOfPartitions, numberOfKeys) with ColPartition
    }
  }
}
