package glint.models.server

import akka.routing.Routee
import glint.messages.server.request.{PullVector, PushVectorFloat}
import glint.messages.server.response.ResponseFloat
import glint.serialization.SerializableHadoopConfiguration
import spire.implicits._

import scala.collection.immutable

private[glint] class PartialVectorFloat(partitionId: Int,
                                        size: Int,
                                        hdfsPath: Option[String],
                                        hadoopConfig: Option[SerializableHadoopConfiguration])
  extends PartialVector[Float](partitionId, size, hdfsPath,  hadoopConfig) {

  override var data: Array[Float] = _

  override def preStart(): Unit = {
    data = loadOrInitialize(new Array[Float](size))
  }

  private def floatReceive: Receive = {
    case pull: PullVector =>
      sender ! ResponseFloat(get(pull.keys))
    case push: PushVectorFloat =>
      update(push.keys, push.values)
      updateFinished(push.id)
  }

  override def receive: Receive = floatReceive.orElse(super.receive)
}

private[glint] class PartialVectorFloatRoutee(partitionId: Int, routeeId: Int, data: Array[Float])
  extends PartialVectorRoutee[Float](partitionId, routeeId, data) {

  private def floatReceive: Receive = {
    case pull: PullVector =>
      sender ! ResponseFloat(get(pull.keys))
    case push: PushVectorFloat =>
      update(push.keys, push.values)
      updateFinished(push.id)
  }

  override def receive: Receive = floatReceive.orElse(super.receive)
}

private[glint] class PartialVectorFloatRoutingLogic extends PartialVectorRoutingLogic {
  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {
    message match {
      case push: PushVectorFloat => routees(push.id >> 16)
      case _ => super.select(message, routees)
    }
  }
}