package glint.util.hdfs

import glint.partitioning.Partitioner

/**
 * Metadata of a vector
 * In combination with the vector data it provides enough information to load it again
 */
case class VectorMetadata(size: Long, createPartitioner: (Int, Long) => Partitioner)