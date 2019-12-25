package glint.models.server

import glint.messages.server.request.{PullVector, PushVectorInt}
import glint.messages.server.response.ResponseInt
import glint.serialization.SerializableHadoopConfiguration
import spire.implicits._

private[glint] class PartialVectorInt(partitionId: Int,
                                      size: Int,
                                      hdfsPath: Option[String],
                                      hadoopConfig: Option[SerializableHadoopConfiguration])
  extends PartialVector[Int](partitionId, size, hdfsPath, hadoopConfig) {

  override var data: Array[Int] = _

  override def preStart(): Unit = {
    data = loadOrInitialize(new Array[Int](size))
  }

  private def intReceive: Receive = {
    case pull: PullVector =>
      sender ! ResponseInt(get(pull.keys))
    case push: PushVectorInt =>
      update(push.keys, push.values)
      updateFinished(push.id)
  }

  override def receive: Receive = intReceive.orElse(super.receive)
}
