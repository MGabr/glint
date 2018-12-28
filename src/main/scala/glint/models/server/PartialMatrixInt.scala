package glint.models.server

import breeze.linalg.{DenseMatrix, Matrix}
import glint.messages.server.request.{PullMatrix, PullMatrixRows, PushMatrixInt}
import glint.messages.server.response.{ResponseRowsInt, ResponseInt}
import glint.models.server.aggregate.Aggregate
import glint.partitioning.Partition
import spire.implicits._

/**
  * A partial matrix holding integers
  *
  * @param partition The partition
  * @param rows The number of rows
  * @param cols The number of columns
  */
private[glint] class PartialMatrixInt(partition: Partition,
                                      rows: Int,
                                      cols: Int,
                                      aggregate: Aggregate)
  extends PartialMatrix[Int](partition, rows, cols, aggregate) {

  override val data: Array[Int] = Array.fill[Int](rows * cols)(0)

  override def receive: Receive = {
    case pull: PullMatrix => sender ! ResponseInt(get(pull.rows, pull.cols))
    case pull: PullMatrixRows => sender ! ResponseRowsInt(getRows(pull.rows), cols)
    case push: PushMatrixInt =>
      update(push.rows, push.cols, push.values)
      updateFinished(push.id)
    case x => handleLogic(x, sender)
  }

}
