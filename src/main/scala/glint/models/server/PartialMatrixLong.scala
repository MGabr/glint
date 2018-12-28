package glint.models.server

import breeze.linalg.{DenseMatrix, Matrix}
import glint.messages.server.request.{PullMatrix, PullMatrixRows, PushMatrixLong}
import glint.messages.server.response.{ResponseRowsLong, ResponseLong}
import glint.models.server.aggregate.Aggregate
import glint.partitioning.Partition
import spire.implicits._

/**
  * A partial matrix holding longs
  *
  * @param partition The row start index
  * @param rows The number of rows
  * @param cols The number of columns
  */
private[glint] class PartialMatrixLong(partition: Partition,
                                       rows: Int,
                                       cols: Int,
                                       aggregate: Aggregate)
  extends PartialMatrix[Long](partition, rows, cols, aggregate) {

  override val data: Array[Long] = Array.fill[Long](rows * cols)(0L)

  override def receive: Receive = {
    case pull: PullMatrix => sender ! ResponseLong(get(pull.rows, pull.cols))
    case pull: PullMatrixRows => sender ! ResponseRowsLong(getRows(pull.rows), cols)
    case push: PushMatrixLong =>
      update(push.rows, push.cols, push.values)
      updateFinished(push.id)
    case x => handleLogic(x, sender)
  }
}
