package glint.util.hdfs

import glint.models.server.aggregate.Aggregate
import glint.partitioning.Partitioner
import glint.partitioning.by.PartitionBy.PartitionBy

/**
  * Metadata of a matrix
  * In combination with the matrix data it provides enough information to load it again
  */
case class MatrixMetadata(rows: Long,
                          cols: Long,
                          aggregate: Aggregate,
                          partitionBy: PartitionBy,
                          createPartitioner: (Int, Long) => Partitioner)
