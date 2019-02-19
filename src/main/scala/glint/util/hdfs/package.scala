package glint.util

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Central package for utils related to saving to and loading from HDFS
  */
package object hdfs {

  def saveMetadata(path: String, config: Configuration, metadata: MatrixMetadata): Unit = {
    val fs = FileSystem.get(config)
    val dataPath = path + "/glint/metadata"
    val dataStream = new ObjectOutputStream(fs.create(new Path(dataPath)))
    dataStream.writeObject(metadata)
    dataStream.close()
  }

  def loadMetadata(path: String, config: Configuration): MatrixMetadata = {
    val fs = FileSystem.get(config)
    val dataPath = path + "/glint/metadata"
    val dataStream = new ObjectInputStream(fs.open(new Path(dataPath)))
    val data = dataStream.readObject().asInstanceOf[MatrixMetadata]
    dataStream.close()
    data
  }

  def savePartitionData[V](path: String, config: Configuration, partitionIndex: Int, partitionData: Array[V]): Unit = {
    val fs = FileSystem.get(config)
    val dataPath = path + "/glint/data/" + partitionIndex
    val dataStream = new ObjectOutputStream(fs.create(new Path(dataPath)))
    dataStream.writeObject(partitionData)
    dataStream.close()
  }

  def loadPartitionData[V](path: String, config: Configuration, partitionIndex: Int): Array[V] = {
    val fs = FileSystem.get(config)
    val dataPath = path + "/glint/data/" + partitionIndex
    val dataStream = new ObjectInputStream(fs.open(new Path(dataPath)))
    val data = dataStream.readObject().asInstanceOf[Array[V]]
    dataStream.close()
    data
  }
}
