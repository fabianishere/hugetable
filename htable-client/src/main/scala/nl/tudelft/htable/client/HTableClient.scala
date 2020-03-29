package nl.tudelft.htable.client

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import nl.tudelft.htable.core.{Query, Row, RowMutation}
import nl.tudelft.htable.protocol.ClientAdapters._
import nl.tudelft.htable.protocol.CoreAdapters
import nl.tudelft.htable.protocol.client.ClientServiceClient
import org.apache.curator.framework.CuratorFramework

import scala.concurrent.{ExecutionContextExecutor, Future, Promise}

/**
 * A client interface for accessing and operating on a HTable cluster.
 */
trait HTableClient {

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

  override def read(query: Query): Source[Row, NotUsed] = {
    val rootAddress = CoreAdapters.deserializeAddress(zookeeper.getData.forPath("/root"))
    val client = openClient(rootAddress)
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

  private def openClient(address: InetSocketAddress): ClientServiceClient = {
    val settings = GrpcClientSettings
      .connectToServiceAt(address.getHostString, address.getPort)
      .withTls(false)
    ClientServiceClient(settings)
  }
}
