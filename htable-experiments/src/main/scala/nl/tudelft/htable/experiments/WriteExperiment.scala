package nl.tudelft.htable.experiments

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.util.ByteString
import nl.tudelft.htable.client.HTableClient
import nl.tudelft.htable.core._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.collection.Seq
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._

/**
 * Main class of the HugeTable server program.
 */
object WriteExperiment {

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

    def stop(): Unit = {
      zookeeper.close()
      client.close()
      sys.terminate()
    }

    val col = ByteString("column")
    val value = ByteString("a" * 1000) // We write 1000 byte values per request (same as big table paper)
    val max = 100000000
    var start = System.currentTimeMillis()
    for (i <- 0 until max) {
      val time = System.currentTimeMillis()
      val mutation: RowMutation = RowMutation("test", ByteString("row_" + i))
        .put(RowCell(col, time, value))
      val result = client.mutate(mutation)
      Await.result(result, 100.seconds)
      if ((i % 1000) == 0) {
        val end = System.currentTimeMillis()
        val avg = 1000.0 / ((end - start) / 1000.0)
        start = System.currentTimeMillis()
        System.out.println("At " + i + " requests total avg p/s: " + avg)
      }
    }

    stop()
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
      required = true
    )

    verify()
  }

}
