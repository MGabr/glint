package glint.partitioning

import glint.partitioning.by.PartitionBy.PartitionBy

/**
  * Partitioners allocate a server id for each key
  */
trait Partitioner extends Serializable {

  val partitionBy: PartitionBy

  /**
    * Assign a server to the given key
    *
    * @param key The key to partition
    * @return The partition
    */
  @inline
  def partition(key: Long): Partition

  /**
    * Returns all partitions
    *
    * @return The array of partitions
    */
  def all(): Array[Partition]

}
