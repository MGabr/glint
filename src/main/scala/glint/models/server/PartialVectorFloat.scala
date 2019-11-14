package glint.models.server

import glint.messages.server.request.{PullVector, PushVectorFloat, PushSave}
import glint.messages.server.response.ResponseFloat
import glint.partitioning.Partition
import glint.serialization.SerializableHadoopConfiguration
import glint.util.hdfs
import spire.implicits._

/**
  * A partial vector holding floats
  *
  * @param partition The partition
  */
private[glint] class PartialVectorFloat(partition: Partition,
                                        hdfsPath: Option[String],
                                        hadoopConfig: Option[SerializableHadoopConfiguration])
  extends PartialVector[Float](partition, hdfsPath,  hadoopConfig) {

  override var data: Array[Float] = _

  override def preStart(): Unit = {
    data = loadOrInitialize(new Array[Float](size))
  }

  override def receive: Receive = {
    case pull: PullVector => sender ! ResponseFloat(get(pull.keys))
    case push: PushVectorFloat =>
      update(push.keys, push.values)
      updateFinished(push.id)
    case push: PushSave =>
      save(push.path, push.hadoopConfig)
      updateFinished(push.id)
    case x => handleLogic(x, sender)
  }

}
