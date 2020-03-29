package nl.tudelft.htable.server.cli

import java.io.File
import java.util.UUID

import akka.actor.typed.ActorSystem
import com.typesafe.config.ConfigFactory
import nl.tudelft.htable.server.core.HTableServer
import nl.tudelft.htable.storage.mem.InMemoryStorageDriver
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
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

    val driver = new InMemoryStorageDriver()
    ActorSystem(HTableServer(UUID.randomUUID().toString, zookeeper, driver), "htable", actorConf)
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
