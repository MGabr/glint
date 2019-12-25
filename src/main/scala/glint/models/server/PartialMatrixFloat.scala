package glint.models.server

import akka.routing.Routee
import glint.messages.server.request.{PullMatrix, PullMatrixRows, PushMatrixFloat}
import glint.messages.server.response.{ResponseFloat, ResponseRowsFloat}
import glint.serialization.SerializableHadoopConfiguration
import spire.implicits._

import scala.collection.immutable

private[glint] class PartialMatrixFloat(partitionId: Int,
                                        rows: Int,
                                        cols: Int,
                                        hdfsPath: Option[String],
                                        hadoopConfig: Option[SerializableHadoopConfiguration])
  extends PartialMatrix[Float](partitionId, rows, cols, hdfsPath, hadoopConfig) {

  override var data: Array[Float] = _

  override def preStart(): Unit = {
    data = loadOrInitialize(Array.fill[Float](rows * cols)(0.0f))
  }

  private def floatReceive: Receive = {
    case pull: PullMatrix =>
      sender ! ResponseFloat(get(pull.rows, pull.cols))
    case pull: PullMatrixRows =>
      sender ! ResponseRowsFloat(getRows(pull.rows), cols)
    case push: PushMatrixFloat =>
      update(push.rows, push.cols, push.values)
      updateFinished(push.id)
  }

  override def receive: Receive = floatReceive.orElse(super.receive)
}

private[glint] class PartialMatrixFloatRoutee(routeeId: Int,
                                              partitionId: Int,
                                              rows: Int,
                                              cols: Int,
                                              data: Array[Float])
  extends PartialMatrixRoutee[Float](routeeId, partitionId, rows, cols, data) {

  private def floatReceive: Receive = {
    case pull: PullMatrix =>
      sender ! ResponseFloat(get(pull.rows, pull.cols))
    case pull: PullMatrixRows =>
      sender ! ResponseRowsFloat(getRows(pull.rows), cols)
    case push: PushMatrixFloat =>
      update(push.rows, push.cols, push.values)
      updateFinished(push.id)
  }

  override def receive: Receive = floatReceive.orElse(super.receive)
}

private[glint] class PartialMatrixFloatRoutingLogic extends PartialMatrixRoutingLogic {

  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {
    message match {
      case push: PushMatrixFloat => routees(push.id >> 16)
      case _ => super.select(message, routees)
    }
  }
}