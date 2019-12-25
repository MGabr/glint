package glint.models.server

import glint.messages.server.request.{PullMatrix, PullMatrixRows, PushMatrixDouble}
import glint.messages.server.response.{ResponseDouble, ResponseRowsDouble}
import glint.serialization.SerializableHadoopConfiguration
import spire.implicits._

private[glint] class PartialMatrixDouble(partitionId: Int,
                                         rows: Int,
                                         cols: Int,
                                         hdfsPath: Option[String],
                                         hadoopConfig: Option[SerializableHadoopConfiguration])
  extends PartialMatrix[Double](partitionId, rows, cols, hdfsPath, hadoopConfig) {

  override var data: Array[Double] = _

  override def preStart(): Unit = {
    data = loadOrInitialize(Array.fill[Double](rows * cols)(0.0))
  }

  private def doubleReceive: Receive = {
    case pull: PullMatrix =>
      sender ! ResponseDouble(get(pull.rows, pull.cols))
    case pull: PullMatrixRows =>
      sender ! ResponseRowsDouble(getRows(pull.rows), cols)
    case push: PushMatrixDouble =>
      update(push.rows, push.cols, push.values)
      updateFinished(push.id)
  }

  override def receive: Receive = doubleReceive.orElse(super.receive)
}