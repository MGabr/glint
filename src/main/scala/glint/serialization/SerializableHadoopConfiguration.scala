package glint.serialization

import org.apache.hadoop.conf.Configuration

/**
  * Serializable wrapper around a Hadoop configuration
  * This only serializes the explicitly set values, not any set in site/default or other XML resources.
  *
  * @param conf The hadoop configuration
  */
class SerializableHadoopConfiguration(var conf: Configuration) extends Serializable {

  def get(): Configuration = conf

  private def writeObject(out: java.io.ObjectOutputStream): Unit = {
    conf.write(out)
  }

  private def readObject(in: java.io.ObjectInputStream): Unit = {
    conf = new Configuration()
    conf.readFields(in)
  }

  private def readObjectNoData(): Unit = {
    conf = new Configuration()
  }
}
