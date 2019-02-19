package glint.models.server

import glint.messages.server.request.{PullMatrix, PullMatrixRows, PushMatrixDouble, PushSave}
import glint.messages.server.response.{ResponseDouble, ResponseRowsDouble}
import glint.models.server.aggregate.Aggregate
import glint.partitioning.Partition
import glint.serialization.SerializableHadoopConfiguration
import spire.implicits._

/**
  * A partial matrix holding doubles
  *
  * @param partition The partition
  * @param rows The number of rows
  * @param cols The number of columns
  * @param aggregate The type of aggregation to apply
  * @param hdfsPath The HDFS base path from which the partial matrix' initial data should be loaded from
  * @param hadoopConfig The serializable Hadoop configuration to use for loading the initial data from HDFS
  */
private[glint] class PartialMatrixDouble(partition: Partition,
                                         rows: Int,
                                         cols: Int,
                                         aggregate: Aggregate,
                                         hdfsPath: Option[String],
                                         hadoopConfig: Option[SerializableHadoopConfiguration])
  extends PartialMatrix[Double](partition, rows, cols, aggregate, hdfsPath, hadoopConfig) {

  override val data: Array[Double] = loadOrInitialize(() => Array.fill[Double](rows * cols)(0.0))

  override def receive: Receive = {
    case pull: PullMatrix => sender ! ResponseDouble(get(pull.rows, pull.cols))
    case pull: PullMatrixRows => sender ! ResponseRowsDouble(getRows(pull.rows), cols)
    case push: PushMatrixDouble =>
      update(push.rows, push.cols, push.values)
      updateFinished(push.id)
    case push: PushSave =>
      save(push.path, push.hadoopConfig)
      updateFinished(push.id)
    case x => handleLogic(x, sender)
  }

}
