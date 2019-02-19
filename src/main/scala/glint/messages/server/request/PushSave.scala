package glint.messages.server.request

import glint.serialization.SerializableHadoopConfiguration

/**
  * A push save request
  *
  * @param id The push identification
  * @param path The HDFS base path where the matrix should be saved
  * @param hadoopConfig The serializable Hadoop configuration to use for saving the data to HDFS
  */
private[glint] case class PushSave(id: Integer,
                                   path: String,
                                   hadoopConfig: SerializableHadoopConfiguration) extends Request

