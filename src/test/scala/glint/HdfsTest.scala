package glint

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.scalatest.{BeforeAndAfterAll, TestSuite}

/**
  * Starts a HDFS cluster before the tests and shuts it down afterwards.
  * Provides a Hadoop configuration to access this cluster
  */
trait HdfsTest extends BeforeAndAfterAll { this: TestSuite =>

  /**
    * The Hadoop configuration to access the cluster
    */
  val hadoopConfig = new Configuration()

  private var hdfsCluster: MiniDFSCluster = null

  override def beforeAll(): Unit = {
    super.beforeAll()

    val baseDir = new File("./target/hdfs").getAbsoluteFile
    FileUtil.fullyDelete(baseDir)
    hadoopConfig.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath())
    hdfsCluster = new MiniDFSCluster.Builder(hadoopConfig).build()
  }

  override def afterAll(): Unit = {
    super.afterAll()

    hdfsCluster.shutdown()
  }

}
