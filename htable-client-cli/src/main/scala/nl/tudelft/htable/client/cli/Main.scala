package nl.tudelft.htable.client.cli

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.util.ByteString
import nl.tudelft.htable.client.HTableClient
import nl.tudelft.htable.core._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.collection.Seq
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

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

    def stop(): Unit = {
      zookeeper.close()
      client.close()
      sys.terminate()
    }

    conf.subcommand match {
      case Some(conf.get) =>
        client
          .read(Get(conf.get.table(), ByteString(conf.get.key())))
          .runForeach(printRow)
          .onComplete {
            case Failure(exception) =>
              exception.printStackTrace()
              stop()
            case Success(_) => stop()
          }
      case Some(conf.scan) =>
        val scan = Scan(conf.scan.table(), RowRange(conf.scan.startKey(), conf.scan.endKey()), conf.scan.reversed())
        client
          .read(scan)
          .runForeach(printRow)
          .onComplete {
            case Failure(exception) =>
              exception.printStackTrace()
              stop()
            case Success(_) => stop()
          }
      case Some(conf.put) =>
        val time = conf.put.timestamp.getOrElse(System.currentTimeMillis())
        val mutation: RowMutation = conf.put.cells
          .foldLeft(RowMutation(conf.put.table(), conf.put.key())) {
            case (acc: RowMutation, (qualifier, value)) =>
              acc.put(RowCell(ByteString(qualifier), time, ByteString(value)))
          }
        client
          .mutate(mutation)
          .onComplete {
            case Failure(exception) =>
              exception.printStackTrace()
              stop()
            case Success(_) => stop()
          }
      case Some(conf.delete) =>
        val mutation = conf.delete.qualifier.toOption match {
          case Some(value) =>
            RowMutation(conf.delete.table(), conf.delete.key())
              .delete(RowCell(ByteString(value), 0, ByteString.empty))
          case None =>
            RowMutation(conf.delete.table(), conf.delete.key()).delete()
        }
        client
          .mutate(mutation)
          .onComplete {
            case Failure(exception) =>
              exception.printStackTrace()
              stop()
            case Success(_) => stop()
          }
      case Some(conf.createTable) =>
        client
          .create(conf.createTable.table())
          .onComplete {
            case Failure(exception) =>
              exception.printStackTrace()
              stop()
            case Success(_) => stop()
          }
      case Some(conf.deleteTable) =>
        client
          .delete(conf.createTable.table())
          .onComplete {
            case Failure(exception) =>
              exception.printStackTrace()
              stop()
            case Success(_) => stop()
          }
      case Some(conf.split) =>
        client
          .split(Tablet(conf.split.table(), RowRange.leftBounded(conf.split.startKey())), conf.split.splitKey())
          .onComplete {
            case Failure(exception) =>
              exception.printStackTrace()
              stop()
            case Success(_) => stop()
          }
      case _ =>
        conf.printHelp()
        stop()

    }
  }

  /**
   * Print the contents of a row.
   */
  private def printRow(row: Row): Unit = {
    row.cells.foreach { cell =>
      println(s"${row.key.utf8String}\t${cell.qualifier.utf8String}\t${cell.timestamp}\t${cell.value.utf8String}")
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
      required = true
    )

    /**
     * A command to obtain the value of a single row.
     */
    val get = new Subcommand("get") {

      /**
       * The table to read from.
       */
      val table = trailArg[String](required = true)

      /**
       * The key to read.
       */
      val key = trailArg[String](required = true)
    }
    addSubcommand(get)

    /**
     * A command to perform a scan of a table.
     */
    val scan = new Subcommand("scan") {

      /**
       * The table to read from.
       */
      val table = trailArg[String](required = true)

      /**
       * The start key to scan from.
       */
      val startKey = opt[String](default = Some(""), required = false).map(s => ByteString(s))

      /**
       * The end key to scan from.
       */
      val endKey = opt[String](default = Some(""), required = false).map(s => ByteString(s))

      /**
       * A flag to make the scan reversed.
       */
      val reversed = opt[Boolean](default = Some(false))
    }
    addSubcommand(scan)

    /**
     * Add a row to a table.
     */
    val put = new Subcommand("put") {

      /**
       * The table to add the row to.
       */
      val table = trailArg[String](required = true)

      /**
       * The key to delete
       */
      val key = trailArg[String]().map(s => ByteString(s))

      /**
       * The cell to append.
       */
      val cells = propsLong[String]("cells")

      /**
       * The timestamp of the row.
       */
      val timestamp = opt[Long](short = 't')
    }
    addSubcommand(put)

    /**
     * Delete a row from a table.
     */
    val delete = new Subcommand("delete") {

      /**
       * The table to delete the row/cell from.
       */
      val table = trailArg[String](required = true)

      /**
       * The key to delete
       */
      val key = trailArg[String](default = Some("")).map(s => ByteString(s))

      /**
       * The cell to delete
       */
      val qualifier = opt[String](required = false).map(s => ByteString(s))
    }
    addSubcommand(delete)

    /**
     * A command to perform a table creation.
     */
    val createTable = new Subcommand("create-table") {

      /**
       * The table to create
       */
      val table = trailArg[String](required = true)
    }
    addSubcommand(createTable)

    /**
     * A command to perform a table deletion.
     */
    val deleteTable = new Subcommand("delete-table") {

      /**
       * The table to create
       */
      val table = trailArg[String](required = true)
    }
    addSubcommand(deleteTable)

    /**
     * A command to perform a tablet split.
     */
    val split = new Subcommand("split") {

      /**
       * The table to split.
       */
      val table = trailArg[String](required = true)

      /**
       * The start key of the tablet to split
       */
      val startKey = opt[String](default = Some("")).map(s => ByteString(s))

      /**
       * The split point.
       */
      val splitKey = opt[String](default = Some("")).map(s => ByteString(s))
    }
    addSubcommand(split)

    verify()
  }
}
