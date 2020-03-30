package nl.tudelft.htable.client

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import akka.{Done, NotUsed}
import nl.tudelft.htable.core.{Get, Query, Row, RowMutation, RowRange, Scan}
import nl.tudelft.htable.protocol.ClientAdapters._
import nl.tudelft.htable.protocol.CoreAdapters
import nl.tudelft.htable.protocol.admin.{AdminServiceClient, CreateTableRequest, CreateTableResponse, DeleteTableRequest}
import nl.tudelft.htable.protocol.client.ClientServiceClient
import org.apache.curator.framework.CuratorFramework

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}

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

  override def create(name: String): Future[Done] =  {
    val client = openAdmin()
    client.createTable(CreateTableRequest(name)).map(_ => Done)
  }

  override def delete(name: String): Future[Done] = {
    val client = openAdmin()
    client.deleteTable(DeleteTableRequest(name))
      .map(_ => Done)
  }

  override def read(query: Query): Source[Row, NotUsed] = {
    val address = query match {
      case Get(table, key) => resolveLocation(table, key)
      case Scan(table, range, _) => resolveLocation(table, range.start)
    }

    address match {
      case Some(value) =>
        val client = openClient(value)
        read(query, client)
      case None =>
        Source.empty
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

  private def resolveLocation(table: String, startKey: ByteString): Option[InetSocketAddress] = {
    val rootAddress = CoreAdapters.deserializeAddress(zookeeper.getData.forPath("/root"))

    if (table.equalsIgnoreCase("METADATA") && startKey.isEmpty) {
      return Some(rootAddress)
    }

    val rootClient = openClient(rootAddress)

    // Append a single character to range since it is exclusive
    val key = startKey ++ ByteString("a")
    val metaKey = if (table.equalsIgnoreCase("METADATA"))
      ByteString("METADATA") ++ key
    else
      ByteString("METADATA" ++ table) ++ key

    val meta = read(Scan("METADATA", RowRange.rightBounded(metaKey), reversed = true), rootClient)
      .runWith(Sink.headOption)

    Await.result(meta, 5.seconds) match {
      case Some(value) =>
        val metaAddress = CoreAdapters.deserializeAddress(value.cells.find(_.qualifier == "node").get.value.toArray)
        val metaClient = if (metaAddress == rootAddress) rootClient else openClient(metaAddress)
        val tabletKey = ByteString(table) ++ key
        val tablet = read(Scan("METADATA", RowRange.rightBounded(tabletKey), reversed = true), metaClient)
          .runWith(Sink.headOption)

        try {
          Await.result(tablet, 5.seconds)
            .map(value => CoreAdapters.deserializeAddress(value.cells.find(_.qualifier == "node").get.value.toArray))
        } finally  {
          rootClient.close()
          metaClient.close()
        }
      case None => None
    }
  }

  private def openClient(address: InetSocketAddress): ClientServiceClient = {
    val settings = GrpcClientSettings
      .connectToServiceAt(address.getHostString, address.getPort)
      .withTls(false)
    ClientServiceClient(settings)
  }

  private def openAdmin(): AdminServiceClient = {
    val masterUid = new String(zookeeper.getData.forPath("/leader"), StandardCharsets.UTF_8)
    val address = CoreAdapters.deserializeAddress(zookeeper.getData.forPath(s"/server/$masterUid"))
    val settings = GrpcClientSettings
      .connectToServiceAt(address.getHostString, address.getPort)
      .withTls(false)
    AdminServiceClient(settings)
  }
}
