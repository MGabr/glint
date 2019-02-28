package glint.messages.server.request

import glint.serialization.SerializableHadoopConfiguration

/**
  * A push save request
  *
  * @param id The push identification
  * @param path The HDFS base path where the matrix should be saved
  * @param hadoopConfig The serializable Hadoop configuration to use for saving the data to HDFS
  * @param trainable Whether the saved matrix should be retrainable
  */
private[glint] case class PushSaveTrainable(id: Integer,
                                            path: String,
                                            hadoopConfig: SerializableHadoopConfiguration,
                                            trainable: Boolean) extends Request
