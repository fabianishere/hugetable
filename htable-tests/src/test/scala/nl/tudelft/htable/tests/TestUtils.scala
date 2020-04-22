package nl.tudelft.htable.tests

import java.nio.file.Files

import org.apache.curator.test.TestingServer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster

/**
 * Utilities for managing tests.
 */
object TestUtils {
  /**
   * The current active HDFS cluster.
   */
  private var dfsCluster: MiniDFSCluster = _

  /**
   * Create a mini Hadoop HDFS cluster.
   */
  def startHDFS(): MiniDFSCluster = {
    val hdfsConf = new Configuration()
    hdfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, Files.createTempDirectory("htable").toString)
    hdfsConf.setBoolean("dfs.webhdfs.enabled", true)

    val builder = new MiniDFSCluster.Builder(hdfsConf)
    dfsCluster = builder
      .manageNameDfsDirs(true)
      .manageDataDfsDirs(true)
      .format(true)
      .build()
    dfsCluster
  }

  /**
   * Obtain the HDFS cluster.
   */
  def hdfs: MiniDFSCluster = dfsCluster

  /**
   * Stop the mini Hadoop cluster.
   */
  def stopHDFS(): Unit = {
    dfsCluster.close()
    dfsCluster = null
  }

  /**
   * The current active ZooKeeper test cluster.
   */
  private var zkTestingServer: TestingServer = _

  /**
   * Create a ZooKeeper test cluster.
   */
  def startZooKeeper(): TestingServer = {
    zkTestingServer = new TestingServer()
    zkTestingServer.start()
    zkTestingServer
  }

  /**
   * Obtain the ZooKeeper testing server.
   */
  def zookeeper: TestingServer = zkTestingServer

  /**
   * Stop a ZooKeeper cluster.
   */
  def stopZooKeeper(): Unit = {
    zkTestingServer.close()
    zkTestingServer = null
  }
}
