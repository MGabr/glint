package glint.models.server

import glint.messages.server.request.{PullVector, PushVectorInt, PushSave}
import glint.messages.server.response.ResponseInt
import glint.partitioning.Partition
import glint.serialization.SerializableHadoopConfiguration
import glint.util.hdfs
import spire.implicits._

/**
  * A partial vector holding integers
  *
  * @param partition The partition
  */
private[glint] class PartialVectorInt(partition: Partition,
                                      hdfsPath: Option[String],
                                      hadoopConfig: Option[SerializableHadoopConfiguration])
  extends PartialVector[Int](partition, hdfsPath, hadoopConfig) {

  override var data: Array[Int] = _

  override def preStart(): Unit = {
    data = loadOrInitialize(new Array[Int](size))
  }

  override def receive: Receive = {
    case pull: PullVector => sender ! ResponseInt(get(pull.keys))
    case push: PushVectorInt =>
      update(push.keys, push.values)
      updateFinished(push.id)
    case push: PushSave =>
      save(push.path, push.hadoopConfig)
      updateFinished(push.id)
    case x => handleLogic(x, sender)
  }

}
