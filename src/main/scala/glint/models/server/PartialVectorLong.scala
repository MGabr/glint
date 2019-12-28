package glint.models.server

import glint.messages.server.request.{PullVector, PushVectorLong, PushSave}
import glint.messages.server.response.ResponseLong
import glint.partitioning.Partition
import glint.serialization.SerializableHadoopConfiguration
import glint.util.hdfs
import spire.implicits._

/**
  * A partial vector holding longs
  *
  * @param partition The partition
  */
private[glint] class PartialVectorLong(partition: Partition,
                                       hdfsPath: Option[String],
                                       hadoopConfig: Option[SerializableHadoopConfiguration])
  extends PartialVector[Long](partition, hdfsPath, hadoopConfig) {

  override var data: Array[Long] = _

  override def preStart(): Unit = {
    data = loadOrInitialize(new Array[Long](size))
  }

  override def receive: Receive = {
    case pull: PullVector => sender ! ResponseLong(get(pull.keys))
    case push: PushVectorLong =>
      update(push.keys, push.values)
      updateFinished(push.id)
    case push: PushSave =>
      save(push.path, push.hadoopConfig)
      updateFinished(push.id)
    case x => handleLogic(x, sender)
  }

}
