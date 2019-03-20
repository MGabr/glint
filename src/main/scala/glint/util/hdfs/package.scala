package glint.util

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Central package for utils related to saving to and loading from HDFS
  */
package object hdfs {

  private def save(path: String, config: Configuration, data: Object): Unit = {
    val fs = FileSystem.get(config)
    val dataStream = new ObjectOutputStream(fs.create(new Path(path)))
    dataStream.writeObject(data)
    dataStream.close()
  }

  private def load(path: String, config: Configuration): Object = {
    val fs = FileSystem.get(config)
    val dataStream = new ObjectInputStream(fs.open(new Path(path)))
    val data = dataStream.readObject()
    dataStream.close()
    data
  }

  def saveMatrixMetadata(path: String,
                         config: Configuration,
                         metadata: MatrixMetadata,
                         pathPostfix: String = "/glint/metadata"): Unit = {
    save(path + pathPostfix, config, metadata)
  }

  def loadMatrixMetadata(path: String,
                         config: Configuration,
                         pathPostfix: String = "/glint/metadata"): MatrixMetadata = {
    load(path + pathPostfix, config).asInstanceOf[MatrixMetadata]
  }

  def saveWord2VecMatrixMetadata(path: String,
                                 config: Configuration,
                                 metadata: Word2VecMatrixMetadata,
                                 pathPostfix: String = "/glint/metadata"): Unit = {
    save(path + pathPostfix, config, metadata)
  }

  def loadWord2VecMatrixMetadata(path: String,
                                 config: Configuration,
                                 pathPostfix: String = "/glint/metadata"): Word2VecMatrixMetadata = {
    load(path + pathPostfix, config).asInstanceOf[Word2VecMatrixMetadata]
  }

  def saveTmpWord2VecMatrixMetadata(config: Configuration, metadata: Word2VecMatrixMetadata): String = {
    val randomUUID = UUID.randomUUID().toString
    val path = s"/tmp/glint/$randomUUID"
    hdfs.saveWord2VecMatrixMetadata(path, config, metadata)
    FileSystem.get(config).deleteOnExit(new Path(path))

    path
  }

  def savePartitionData[V](path: String,
                           config: Configuration,
                           partitionIndex: Int,
                           partitionData: Array[V],
                           pathPostfix: String = "/glint/data/"): Unit = {
    save(path + pathPostfix + partitionIndex, config, partitionData)
  }

  def loadPartitionData[V](path: String,
                           config: Configuration,
                           partitionIndex: Int,
                           pathPostfix: String = "/glint/data/"): Array[V] = {
    load(path + pathPostfix + partitionIndex, config).asInstanceOf[Array[V]]
  }

  def countPartitionData(path: String, config: Configuration, pathPostfix: String = "/glint/data/"): Int = {
    FileSystem.get(config).getContentSummary(new Path(path + pathPostfix)).getFileCount.toInt
  }
}
