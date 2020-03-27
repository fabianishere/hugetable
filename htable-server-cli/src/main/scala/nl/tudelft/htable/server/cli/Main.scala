package nl.tudelft.htable.server.cli

import java.io.File
import java.util.UUID

import akka.actor.typed.ActorSystem
import com.typesafe.config.ConfigFactory
import nl.tudelft.htable.server.core.HTableServer
import nl.tudelft.htable.storage.hbase.HBaseStorageDriver
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.collection.Seq
import scala.util.Properties

/**
 * Main class of the HugeTable server program.
 */
object Main {

  /**
   * Main entry point of the program.
   *
   * @param args The command line arguments passed to the program.
   */
  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val connectionString = conf.zookeeper.getOrElse(List()).mkString(",")
    val zookeeper = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3))

    // Important: enable HTTP/2 in ActorSystem's config
    // We do it here programmatically, but you can also set it in the application.conf
    val actorConf = ConfigFactory
      .parseString("akka.http.server.preview.enable-http2 = on")
      .withFallback(ConfigFactory.defaultApplication())

    val cluster = startHDFS()
    val driver = new HBaseStorageDriver(cluster.getFileSystem)
    ActorSystem(HTableServer(UUID.randomUUID().toString, zookeeper, driver), "htable", actorConf)
  }

  /**
   * Start a HDFS mini cluster.
   */
  private def startHDFS(): MiniDFSCluster = {
    println("Starting HDFS Cluster...")
    val baseDir = new File("miniHDFS")
    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    conf.setBoolean("dfs.webhdfs.enabled", true)
    val builder = new MiniDFSCluster.Builder(conf)
    val hdfsCluster = builder.nameNodePort(9000).manageNameDfsDirs(true).manageDataDfsDirs(true).format(true).build()
    hdfsCluster.waitClusterUp()
    hdfsCluster
  }

  /**
   * The command line configuration of the application.
   *
   * @param arguments The command line arguments passed to the program.
   */
  private class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {

    /**
     * An option for specifying the ZooKeeper addresses to connect to.
     */
    val zookeeper: ScallopOption[List[String]] = opt[List[String]](
      short = 'z',
      descr = "The ZooKeeper addresses to connect to",
      default = Properties.envOrNone("ZOOKEEPER").map(e => List(e)))
    verify()
  }
}
