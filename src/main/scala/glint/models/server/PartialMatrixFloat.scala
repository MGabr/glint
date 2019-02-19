package glint.models.server

import glint.messages.server.request.{PullMatrix, PullMatrixRows, PushMatrixFloat, PushSave}
import glint.messages.server.response.{ResponseFloat, ResponseRowsFloat}
import glint.models.server.aggregate.Aggregate
import glint.partitioning.Partition
import glint.serialization.SerializableHadoopConfiguration
import spire.implicits._

/**
  * A partial matrix holding floats
  *
  * @param partition The partition
  * @param rows The number of rows
  * @param cols The number of columns
  * @param aggregate The type of aggregation to apply
  * @param hdfsPath The HDFS base path from which the partial matrix' initial data should be loaded from
  * @param hadoopConfig The serializable Hadoop configuration to use for loading the initial data from HDFS
  */
private[glint] class PartialMatrixFloat(partition: Partition,
                                        rows: Int,
                                        cols: Int,
                                        aggregate: Aggregate,
                                        hdfsPath: Option[String],
                                        hadoopConfig: Option[SerializableHadoopConfiguration])
  extends PartialMatrix[Float](partition, rows, cols, aggregate, hdfsPath, hadoopConfig) {

  override val data: Array[Float] = loadOrInitialize(() => Array.fill[Float](rows * cols)(0.0f))

  override def receive: Receive = {
    case pull: PullMatrix => sender ! ResponseFloat(get(pull.rows, pull.cols))
    case pull: PullMatrixRows => sender ! ResponseRowsFloat(getRows(pull.rows), cols)
    case push: PushMatrixFloat =>
      update(push.rows, push.cols, push.values)
      updateFinished(push.id)
    case push: PushSave =>
      save(push.path, push.hadoopConfig)
      updateFinished(push.id)
    case x => handleLogic(x, sender)
  }

}
