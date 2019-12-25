package glint.models.server

import glint.messages.server.request.{PullMatrix, PullMatrixRows, PushMatrixInt}
import glint.messages.server.response.{ResponseInt, ResponseRowsInt}
import glint.serialization.SerializableHadoopConfiguration
import spire.implicits._

private[glint] class PartialMatrixInt(partitionId: Int,
                                      rows: Int,
                                      cols: Int,
                                      hdfsPath: Option[String],
                                      hadoopConfig: Option[SerializableHadoopConfiguration])
  extends PartialMatrix[Int](partitionId, rows, cols, hdfsPath, hadoopConfig) {

  override var data: Array[Int] = _

  override def preStart(): Unit = {
    data = loadOrInitialize(Array.fill[Int](rows * cols)(0))
  }

  private def intReceive: Receive = {
    case pull: PullMatrix =>
      sender ! ResponseInt(get(pull.rows, pull.cols))
    case pull: PullMatrixRows =>
      sender ! ResponseRowsInt(getRows(pull.rows), cols)
    case push: PushMatrixInt =>
      update(push.rows, push.cols, push.values)
      updateFinished(push.id)
  }

  override def receive: Receive = intReceive.orElse(super.receive)
}