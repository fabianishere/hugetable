package nl.tudelft.htable.client.cli

import akka.actor.ActorSystem
import akka.stream.Materializer
import nl.tudelft.htable.client.{HTableClient, Query}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.collection.Seq
import scala.concurrent.ExecutionContextExecutor
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
    val zookeeper = CuratorFrameworkFactory.newClient(
      connectionString,
      new ExponentialBackoffRetry(1000, 3)
    )
    zookeeper.start()
    zookeeper.blockUntilConnected()

    implicit val sys: ActorSystem = ActorSystem("client")
    implicit val mat: Materializer = Materializer(sys)
    implicit val ec: ExecutionContextExecutor = sys.dispatcher

    val client = HTableClient(zookeeper)
    client
      .read(Query("metadata"))
      .log("error logging")
      .runForeach(e => println(e))
      .onComplete(_ => client.close())
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
      default = Properties.envOrNone("ZOOKEEPER").map(e => List(e))
    )
    verify()
  }
}
