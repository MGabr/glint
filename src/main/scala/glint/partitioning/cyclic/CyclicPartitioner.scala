package glint.partitioning.cyclic

import glint.partitioning.by.PartitionBy.PartitionBy
import glint.partitioning._
import glint.partitioning.by.PartitionBy

/**
  * A cyclic key partitioner
  *
  * @param partitions The partitions of this cyclic partitioner
  * @param keys The number of keys
  * @param partitionBy The key type by which the partitions are partitioned
  */
private[partitioning] class CyclicPartitioner(partitions: Array[Partition],
                                              keys: Long,
                                              val partitionBy: PartitionBy) extends Partitioner {

  /**
    * Assign a server to the given key according to a cyclic modulo partitioning scheme
    *
    * @param key The key to partition
    * @return The partition
    */
  @inline
  override def partition(key: Long): Partition = {
    if (key >= keys) { throw new IndexOutOfBoundsException() }
    partitions((key % partitions.length).toInt)
  }

  /**
    * Returns all partitions
    *
    * @return The array of partitions
    */
  override def all(): Array[Partition] = partitions
}

object CyclicPartitioner {

  /**
    * Creates a CyclicPartitioner for given number of partitions and keys
    *
    * @param numberOfPartitions The number of partitions
    * @param numberOfKeys The number of keys
    * @param by The key type for partitioning
    * @return A CyclicPartitioner
    */
  def apply(numberOfPartitions: Int, numberOfKeys: Long, by: PartitionBy = PartitionBy.ROW): CyclicPartitioner = {
    val partitions = new Array[Partition](numberOfPartitions)
    var i = 0
    while (i < numberOfPartitions) {
      partitions(i) = CyclicPartition(i, numberOfPartitions, numberOfKeys, by)
      i += 1
    }
    new CyclicPartitioner(partitions, numberOfKeys, by)
  }

}