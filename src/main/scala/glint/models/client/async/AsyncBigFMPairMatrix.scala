package glint.models.client.async

import akka.actor.ActorRef
import com.typesafe.config.Config
import glint.models.client.BigFMPairMatrix
import glint.models.server.aggregate.Aggregate
import glint.partitioning.Partitioner

class AsyncBigFMPairMatrix(partitioner: Partitioner,
                           matrices: Array[ActorRef],
                           config: Config,
                           aggregate: Aggregate,
                           rows: Long,
                           cols: Long,
                           val trainable: Boolean)
  extends AsyncBigMatrixFloat(partitioner, matrices, config, aggregate, rows, cols) with BigFMPairMatrix {
  {

  }
