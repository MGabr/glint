package glint.models.server

import glint.messages.server.request.{PullVector, PushVectorDouble}
import glint.messages.server.response.ResponseDouble
import glint.serialization.SerializableHadoopConfiguration
import spire.implicits._

private[glint] class PartialVectorDouble(partitionId: Int,
                                         size: Int,
                                         hdfsPath: Option[String],
                                         hadoopConfig: Option[SerializableHadoopConfiguration])
  extends PartialVector[Double](partitionId, size, hdfsPath, hadoopConfig) {

  override var data: Array[Double] = _

  override def preStart(): Unit = {
    data = loadOrInitialize(new Array[Double](size))
  }

  private def doubleReceive: Receive = {
    case pull: PullVector =>
      sender ! ResponseDouble(get(pull.keys))
    case push: PushVectorDouble =>
      update(push.keys, push.values)
      updateFinished(push.id)
  }

  override def receive: Receive = doubleReceive.orElse(super.receive)
}
