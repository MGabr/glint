package glint.partitioning.range

import glint.partitioning.by.PartitionBy.PartitionBy
import glint.partitioning._
import glint.partitioning.by.PartitionBy

/**
  * A partitioner that partitions keys according to continuous ranges
  *
  * @param partitions The partitions
  * @param numberOfSmallPartitions The number of small partitions
  * @param smallPartitionSize The size of a small partition
  * @param size The number of keys
  * @param partitionBy The key type by which the partitions are partitioned
  */
private[partitioning] class RangePartitioner(val partitions: Array[Partition],
                                             val numberOfSmallPartitions: Int,
                                             val smallPartitionSize: Int,
                                             val size: Long,
                                             val partitionBy: PartitionBy) extends Partitioner {

  val numberOfSmallKeys: Long = numberOfSmallPartitions.toLong * smallPartitionSize.toLong
  val largePartitionSize: Int = smallPartitionSize + 1

  /**
    * Assign a server to the given key
    *
    * @param key The key to partition
    * @return The partition
    */
  @inline
  override def partition(key: Long): Partition = {

    // Key must be within range
    if (key < 0 || key >= size) {
      throw new IndexOutOfBoundsException(key.toString)
    }

    // We use integer division to compute the index, which discards the remainder and is thus equivalent to a floor
    // function
    val index = if (key < numberOfSmallKeys) {
      (key / smallPartitionSize).toInt
    } else {
      (numberOfSmallPartitions + (key - numberOfSmallKeys) / largePartitionSize).toInt
    }

    partitions(index)
  }

  /**
    * Returns all partitions
    *
    * @return The array of partitions
    */
  override def all(): Array[Partition] = partitions
}

object RangePartitioner {

  /**
    * Creates a RangePartitioner for given number of partitions and keys
    *
    * @param numberOfPartitions The number of partitions
    * @param numberOfKeys The number of keys
    * @param by The key type for partitioning
    * @return A RangePartitioner
    */
  def apply(numberOfPartitions: Int, numberOfKeys: Long, by: PartitionBy = PartitionBy.ROW): RangePartitioner = {
    val partitions = new Array[Partition](numberOfPartitions)
    val numberOfLargePartitions = (numberOfKeys % numberOfPartitions).toInt
    val numberOfSmallPartitions = numberOfPartitions - numberOfLargePartitions
    val keysPerSmallPartition = ((numberOfKeys - (numberOfKeys % numberOfPartitions)) / numberOfPartitions).toInt
    var i = 0
    var start: Long = 0
    var end: Long = start + keysPerSmallPartition
    while (i < numberOfPartitions) {
      if (i < numberOfSmallPartitions) {
        partitions(i) = RangePartition(i, start, end, by)
        start += keysPerSmallPartition
        end += keysPerSmallPartition
      } else {
        end += 1
        partitions(i) = RangePartition(i, start, end, by)
        start += keysPerSmallPartition + 1
        end += keysPerSmallPartition
      }
      i += 1
    }
    new RangePartitioner(partitions, numberOfSmallPartitions, keysPerSmallPartition, numberOfKeys, by)
  }

}