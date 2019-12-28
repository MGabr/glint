package glint.models.server

import glint.messages.server.request.{PullVector, PushVectorDouble, PushSave}
import glint.messages.server.response.ResponseDouble
import glint.partitioning.Partition
import glint.serialization.SerializableHadoopConfiguration
import glint.util.hdfs
import spire.implicits._

/**
  * A partial vector holding doubles
  *
  * @param partition The partition
  */
private[glint] class PartialVectorDouble(partition: Partition,
                                         hdfsPath: Option[String],
                                         hadoopConfig: Option[SerializableHadoopConfiguration])
  extends PartialVector[Double](partition, hdfsPath, hadoopConfig) {

  override var data: Array[Double] = _

  override def preStart(): Unit = {
    data = loadOrInitialize(new Array[Double](size))
  }

  override def receive: Receive = {
    case pull: PullVector => sender ! ResponseDouble(get(pull.keys))
    case push: PushVectorDouble =>
      update(push.keys, push.values)
      updateFinished(push.id)
    case push: PushSave =>
      save(push.path, push.hadoopConfig)
      updateFinished(push.id)
    case x => handleLogic(x, sender)
  }

}
