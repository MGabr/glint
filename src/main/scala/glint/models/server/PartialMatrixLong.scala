package glint.models.server

import glint.messages.server.request.{PullMatrix, PullMatrixRows, PushMatrixLong}
import glint.messages.server.response.{ResponseLong, ResponseRowsLong}
import glint.serialization.SerializableHadoopConfiguration
import spire.implicits._

private[glint] class PartialMatrixLong(partitionId: Int,
                                       rows: Int,
                                       cols: Int,
                                       hdfsPath: Option[String],
                                       hadoopConfig: Option[SerializableHadoopConfiguration])
  extends PartialMatrix[Long](partitionId, rows, cols, hdfsPath, hadoopConfig) {

  override var data: Array[Long] = _

  override def preStart(): Unit = {
    data = loadOrInitialize (Array.fill[Long](rows * cols)(0L))
  }

  private def longReceive: Receive = {
    case pull: PullMatrix =>
      sender ! ResponseLong(get(pull.rows, pull.cols))
    case pull: PullMatrixRows =>
      sender ! ResponseRowsLong(getRows(pull.rows), cols)
    case push: PushMatrixLong =>
      update(push.rows, push.cols, push.values)
      updateFinished(push.id)
  }

  override def receive: Receive = longReceive.orElse(super.receive)
}
