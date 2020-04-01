package nl.tudelft.htable.client

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import akka.{Done, NotUsed}
import nl.tudelft.htable.core._
import nl.tudelft.htable.protocol.ClientAdapters._
import nl.tudelft.htable.protocol.CoreAdapters
import nl.tudelft.htable.protocol.admin.{AdminServiceClient, CreateTableRequest, DeleteTableRequest}
import nl.tudelft.htable.protocol.client.ClientServiceClient
import org.apache.curator.framework.CuratorFramework

import scala.collection.mutable
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.Try

/**
 * A client interface for accessing and operating on a HTable cluster.
 */
trait HTableClient {

  /**
   * Create a new table.
   */
  def create(name: String): Future[Done]

  /**
   * Delete a table.
   */
  def delete(name: String): Future[Done]

  /**
   * Query the rows of a table.
   */
  def read(query: Query): Source[Row, NotUsed]

  /**
   * Perform a mutation on a row.
   */
  def mutate(mutation: RowMutation): Future[Done]

  /**
   * Close the connection to the cluster asynchronously and returns a [Future]
   * that completes when the client closed.
   */
  def close(): Future[Done]

  /**
   * Return a [Future] that completes when the client is closed.
   */
  def closed(): Future[Done]
}

object HTableClient {

  /**
   * Construct a [HTableClient] using the given ZooKeeper client.
   */
  def apply(zookeeper: CuratorFramework): HTableClient =
    new HTableClientImpl(zookeeper, ActorSystem("client"))
}

/**
 * Internal implementation of the [HTableClient] trait.
 *
 * @param zookeeper The ZooKeeper client used to connect to the cluster.
 * @param actorSystem The actor system to drive the client.
 */
private class HTableClientImpl(private val zookeeper: CuratorFramework, private val actorSystem: ActorSystem)
    extends HTableClient {
  private val promise = Promise[Done]()

  implicit val sys: ActorSystem = actorSystem
  implicit val mat: Materializer = Materializer(sys)
  implicit val ec: ExecutionContextExecutor = sys.dispatcher

  private val clientCache = new mutable.HashMap[InetSocketAddress, ClientServiceClient]

  override def create(name: String): Future[Done] = {
    val client = openAdmin()
    client.createTable(CreateTableRequest(name)).map(_ => Done)
  }

  override def delete(name: String): Future[Done] = {
    val client = openAdmin()
    client
      .deleteTable(DeleteTableRequest(name))
      .map(_ => Done)
  }

  override def read(query: Query): Source[Row, NotUsed] = {
    val address = query match {
      case Get(table, key)       => resolveLocations(table, RowRange(key, key))
      case Scan(table, range, _) => resolveLocations(table, range)
    }

    address.flatMapConcat {
      case (address, range) =>
        val client = openClient(address)
        val updatedQuery = query match {
          case Scan(table, _, reversed) => Scan(table, range, reversed)
          case _                        => query
        }
        read(updatedQuery, client)
    }
  }

  private def read(query: Query, client: ClientServiceClient): Source[Row, NotUsed] = {
    client.read(query)
  }

  override def mutate(mutation: RowMutation): Future[Done] = {
    val rootAddress = CoreAdapters.deserializeAddress(zookeeper.getData.forPath("/root"))
    val client = openClient(rootAddress)
    client.mutate(mutation).map(_ => Done)
  }

  override def closed(): Future[Done] = promise.future

  override def close(): Future[Done] = {
    zookeeper.close()
    actorSystem
      .terminate()
      .onComplete(t => promise.complete(t.map(_ => Done)))
    promise.future
  }

  private def resolveLocations(table: String, range: RowRange): Source[(InetSocketAddress, RowRange), NotUsed] = {
    val rootAddress = resolveRoot()
    val rootClient = getClient(rootAddress)

    // Append a character to range
    val metaKey =
      if (table.equalsIgnoreCase("METADATA"))
        ByteString("METADATA") ++ range.end ++ ByteString(9)
      else
        ByteString("METADATA" ++ table) ++ range.end ++ ByteString(9)

    val meta: Source[(InetSocketAddress, RowRange), NotUsed] =
      read(Scan("METADATA", RowRange.rightBounded(metaKey), reversed = true), rootClient)
        .takeWhile(row => Order.keyOrdering.gt(row.key, range.start), inclusive = true)
        .fold(List.empty[Row])((acc: List[Row], curr: Row) => (curr :: acc))
        .mapConcat[Row](identity)
        .mapConcat[(InetSocketAddress, RowRange)] { row =>
          val res = for {
            startKey <- row.cells.find(_.qualifier == ByteString("start-key"))
            endKey <- row.cells.find(_.qualifier == ByteString("end-key"))
            range = RowRange(startKey.value, endKey.value)
            nodeCell <- row.cells.find(_.qualifier == ByteString("node"))
            uid = nodeCell.value.utf8String
            metaAddress <- Try { CoreAdapters.deserializeAddress(zookeeper.getData.forPath(s"/servers/$uid")) }.toOption
          } yield (metaAddress, range)
          res.map(Seq(_)).getOrElse(Seq())
        }

    if (table.equalsIgnoreCase("METADATA")) {
      return meta
    }

    meta.flatMapConcat {
      case (address, metaRange) =>
        val metaClient = getClient(address)
        val tabletKey = Order.keyOrdering.min(ByteString(table) ++ range.end, metaRange.end)
        read(Scan("METADATA", RowRange.rightBounded(tabletKey), reversed = true), metaClient)
          .takeWhile(row => Order.keyOrdering.gteq(row.key, range.start))
          .fold(List.empty[Row])((acc: List[Row], curr: Row) => (curr :: acc))
          .mapConcat[Row](identity)
          .mapConcat[(InetSocketAddress, RowRange)] { row =>
            val res = for {
              startKey <- row.cells.find(_.qualifier == ByteString("start-key"))
              endKey <- row.cells.find(_.qualifier == ByteString("end-key"))
              range = RowRange(startKey.value, endKey.value)
              nodeCell <- row.cells.find(_.qualifier == ByteString("node"))
              uid = nodeCell.value.utf8String
              metaAddress <- Try { CoreAdapters.deserializeAddress(zookeeper.getData.forPath(s"/servers/$uid")) }.toOption
            } yield (metaAddress, range)
            res.map(Seq(_)).getOrElse(Seq())
          }
    }
  }

  private def resolveRoot(): InetSocketAddress = {
    CoreAdapters.deserializeAddress(zookeeper.getData.forPath("/root"))
  }

  private def resolveMaster(): InetSocketAddress = {
    val masterUid = new String(zookeeper.getData.forPath("/leader"), StandardCharsets.UTF_8)
    CoreAdapters.deserializeAddress(zookeeper.getData.forPath(s"/server/$masterUid"))
  }

  private def getClient(address: InetSocketAddress): ClientServiceClient = {
    clientCache.getOrElseUpdate(address, openClient(address))
  }

  private def openClient(address: InetSocketAddress): ClientServiceClient = {
    val settings = GrpcClientSettings
      .connectToServiceAt(address.getHostString, address.getPort)
      .withTls(false)
    val client = ClientServiceClient(settings)
    clientCache(address) = client
    client
  }

  private def openAdmin(): AdminServiceClient = {
    val address = resolveMaster()
    val settings = GrpcClientSettings
      .connectToServiceAt(address.getHostString, address.getPort)
      .withTls(false)
    AdminServiceClient(settings)
  }
}
