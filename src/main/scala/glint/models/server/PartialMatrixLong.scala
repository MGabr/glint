package glint.models.server

import glint.messages.server.request.{PullMatrix, PullMatrixRows, PushMatrixLong, PushSave}
import glint.messages.server.response.{ResponseLong, ResponseRowsLong}
import glint.models.server.aggregate.Aggregate
import glint.partitioning.Partition
import glint.serialization.SerializableHadoopConfiguration
import spire.implicits._

/**
  * A partial matrix holding longs
  *
  * @param partition The row start index
  * @param rows The number of rows
  * @param cols The number of columns
  * @param aggregate The type of aggregation to apply
  * @param hdfsPath The HDFS base path from which the partial matrix' initial data should be loaded from
  * @param hadoopConfig The serializable Hadoop configuration to use for loading the initial data from HDFS
  */
private[glint] class PartialMatrixLong(partition: Partition,
                                       rows: Int,
                                       cols: Int,
                                       aggregate: Aggregate,
                                       hdfsPath: Option[String],
                                       hadoopConfig: Option[SerializableHadoopConfiguration])
  extends PartialMatrix[Long](partition, rows, cols, aggregate, hdfsPath, hadoopConfig) {

  override var data: Array[Long] = _

  override def preStart(): Unit = {
    data = loadOrInitialize (Array.fill[Long](rows * cols)(0L))
  }

  override def receive: Receive = {
    case pull: PullMatrix => sender ! ResponseLong(get(pull.rows, pull.cols))
    case pull: PullMatrixRows => sender ! ResponseRowsLong(getRows(pull.rows), cols)
    case push: PushMatrixLong =>
      update(push.rows, push.cols, push.values)
      updateFinished(push.id)
    case push: PushSave =>
      save(push.path, push.hadoopConfig)
      updateFinished(push.id)
    case x => handleLogic(x, sender)
  }
}
