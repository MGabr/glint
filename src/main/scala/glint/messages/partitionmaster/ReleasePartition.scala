package glint.messages.partitionmaster

import glint.partitioning.Partition

/**
  * Message that releases a partition (on failure to start a parameter server using it)
  *
  * @param partition the partition to release
  */
private[glint] case class ReleasePartition(partition: Partition)
