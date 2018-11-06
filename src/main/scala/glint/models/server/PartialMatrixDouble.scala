package glint.models.server

import breeze.linalg.{DenseMatrix, Matrix}
import glint.messages.server.request.{PullMatrix, PullMatrixRows, PushMatrixDouble}
import glint.messages.server.response.{ResponseRowsDouble, ResponseDouble}
import glint.models.server.aggregate.Aggregate
import glint.partitioning.Partition
import spire.implicits._

/**
  * A partial matrix holding doubles
  *
  * @param partition The partition
  * @param cols The number of columns
  */
private[glint] class PartialMatrixDouble(partition: Partition,
                                         cols: Int,
                                         aggregate: Aggregate) extends PartialMatrix[Double](partition, cols, aggregate) {

  //DenseMatrix.zeros[Double](rows, cols)
  override val data: Array[Array[Double]] = Array.fill(rows)(Array.fill[Double](cols)(0.0))

  override def receive: Receive = {
    case pull: PullMatrix => sender ! ResponseDouble(get(pull.rows, pull.cols))
    case pull: PullMatrixRows => sender ! ResponseRowsDouble(getRows(pull.rows), cols)
    case push: PushMatrixDouble =>
      update(push.rows, push.cols, push.values)
      updateFinished(push.id)
    case x => handleLogic(x, sender)
  }

}
