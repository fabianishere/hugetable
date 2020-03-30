package nl.tudelft.htable.client.cli

import akka.actor.ActorSystem
import akka.stream.Materializer
import nl.tudelft.htable.client.HTableClient
import nl.tudelft.htable.core.{RowRange, Scan}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.collection.Seq
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Properties, Success}

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
      .read(Scan("METADATA", RowRange.unbounded))
      .log("error logging")
      .runForeach { row =>
        println(s"KEY: ${row.key.utf8String}")
        row.cells.foreach { cell =>
          println(s"\t${cell.qualifier.utf8String}[${cell.timestamp}] = ${cell.value}")
        }
      }
      .onComplete {
        case Success(value) =>
          println(value)
          client.close()
          sys.terminate()
        case Failure(exception) =>
          exception.printStackTrace()
          client.close()
          sys.terminate()
      }
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
