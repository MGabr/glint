package glint.models.server

import glint.messages.server.request.{PullVector, PushVectorLong}
import glint.messages.server.response.ResponseLong
import glint.serialization.SerializableHadoopConfiguration
import spire.implicits._

private[glint] class PartialVectorLong(partitionId: Int,
                                       size: Int,
                                       hdfsPath: Option[String],
                                       hadoopConfig: Option[SerializableHadoopConfiguration])
  extends PartialVector[Long](partitionId, size, hdfsPath, hadoopConfig) {

  override var data: Array[Long] = _

  override def preStart(): Unit = {
    data = loadOrInitialize(new Array[Long](size))
  }

  private def longReceive: Receive = {
    case pull: PullVector =>
      sender ! ResponseLong(get(pull.keys))
    case push: PushVectorLong =>
      update(push.keys, push.values)
      updateFinished(push.id)
  }

  override def receive: Receive = longReceive.orElse(super.receive)
}
