package nl.tudelft.htable.client

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import nl.tudelft.htable.core.{Get, Node, Order, Query, Row, RowMutation, RowRange, Scan, Tablet}
import nl.tudelft.htable.protocol.CoreAdapters
import nl.tudelft.htable.protocol.admin.{AdminServiceClient, CreateTableRequest, DeleteTableRequest, SplitTableRequest}
import nl.tudelft.htable.protocol.client.{ClientServiceClient, MutateRequest, ReadRequest}
import org.apache.curator.framework.CuratorFramework

import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.Try

/**
 * Internal implementation of the [HTableClient] trait.
 *
 * @param zookeeper The ZooKeeper client used to connect to the cluster.
 * @param actorSystem The actor system to drive the client.
 * @param resolver The resolver used to connect to the nodes.
 */
private class HTableClientImpl(private val zookeeper: CuratorFramework,
                               private val actorSystem: ActorSystem,
                               private val resolver: ServiceResolver)
    extends HTableClient {
  implicit val sys: ActorSystem = actorSystem
  implicit val mat: Materializer = Materializer(sys)
  implicit val ec: ExecutionContextExecutor = sys.dispatcher
  private val promise = Promise[Done]()

  override def create(name: String): Future[Done] = {
    val node = resolveMaster()
    val client = resolver.openAdmin(node)
    client.createTable(CreateTableRequest(name)).map(_ => Done)
  }

  override def delete(name: String): Future[Done] = {
    val node = resolveMaster()
    val client = resolver.openAdmin(node)
    client
      .deleteTable(DeleteTableRequest(name))
      .map(_ => Done)
  }

  override def split(name: String, startKey: ByteString): Future[Done] = {
    val node = resolveMaster()
    val client = resolver.openAdmin(node)
    client
      .splitTable(SplitTableRequest(name, CoreAdapters.akkaToProtobuf(startKey)))
      .map(_ => Done)
  }

  override def read(query: Query): Source[Row, NotUsed] = {
    val nodes = query match {
      case Get(table, key)       => resolve(Tablet(query.table, RowRange(key, key ++ ByteString(9, 9))))
      case Scan(table, range, _) => resolve(Tablet(table, range))
    }

    nodes.flatMapConcat {
      case (node, tablet) =>
        val client = resolver.openClient(node)
        val updatedQuery = query match {
          case Scan(table, _, reversed) => Scan(table, tablet.range, reversed)
          case _                        => query
        }
        read(updatedQuery, client)
    }
  }

  private def read(query: Query, client: ClientServiceClient): Source[Row, NotUsed] = {
    client
      .read(ReadRequest(Some(query)))
      .mapConcat(_.rows)
  }

  override def mutate(mutation: RowMutation): Future[Done] = {
    resolve(Tablet(mutation.table, RowRange(mutation.key, mutation.key ++ ByteString(9, 9))))
      .flatMapConcat {
        case (node, _) =>
          val client = resolver.openClient(node)
          Source.future(client.mutate(MutateRequest(Some(mutation))))
      }
      .runForeach(_ => ())
  }

  override def closed(): Future[Done] = promise.future

  override def close(): Future[Done] = {
    zookeeper.close()
    actorSystem
      .terminate()
      .onComplete(t => promise.complete(t.map(_ => Done)))
    promise.future
  }

  override def resolve(tablet: Tablet): Source[(Node, Tablet), NotUsed] = {
    val rootAddress = resolveRoot()
    val rootClient = resolver.openClient(rootAddress)

    // Append a character to range
    val metaKey =
      if (tablet.table.equalsIgnoreCase("METADATA"))
        ByteString("METADATA") ++ tablet.range.end ++ ByteString(9)
      else
        ByteString("METADATA" ++ tablet.table) ++ tablet.range.end ++ ByteString(9)

    val meta: Source[(Node, Tablet), NotUsed] =
      read(Scan("METADATA", RowRange.rightBounded(metaKey), reversed = true), rootClient)
        .takeWhile(row => Order.keyOrdering.gt(row.key, tablet.range.start), inclusive = true)
        .fold(List.empty[Row])((acc: List[Row], curr: Row) => (curr :: acc))
        .mapConcat[Row](identity)
        .mapConcat[(Node, Tablet)] { row =>
          val res = for {
            startKey <- row.cells.find(_.qualifier == ByteString("start-key"))
            endKey <- row.cells.find(_.qualifier == ByteString("end-key"))
            range = RowRange(startKey.value, endKey.value)
            metaTablet = Tablet(tablet.table, range)
            nodeCell <- row.cells.find(_.qualifier == ByteString("node"))
            uid = nodeCell.value.utf8String
            metaAddress <- Try { CoreAdapters.deserializeAddress(zookeeper.getData.forPath(s"/servers/$uid")) }.toOption
            node = Node(uid, metaAddress)
          } yield (node, metaTablet)
          res.map(Seq(_)).getOrElse(Seq())
        }

    if (tablet.table.equalsIgnoreCase("METADATA")) {
      return meta
    }

    meta.flatMapConcat {
      case (node, metaTablet) =>
        val metaClient = resolver.openClient(node)
        val tabletKey = Order.keyOrdering.min(ByteString(tablet.table) ++ tablet.range.end, metaTablet.range.end)
        read(Scan("METADATA", RowRange.rightBounded(tabletKey), reversed = true), metaClient)
          .takeWhile(row => Order.keyOrdering.gteq(row.key, tablet.range.start))
          .fold(List.empty[Row])((acc: List[Row], curr: Row) => (curr :: acc))
          .mapConcat[Row](identity)
          .mapConcat[(Node, Tablet)] { row =>
            val res = for {
              startKey <- row.cells.find(_.qualifier == ByteString("start-key"))
              endKey <- row.cells.find(_.qualifier == ByteString("end-key"))
              range = RowRange(startKey.value, endKey.value)
              metaTablet = Tablet(tablet.table, range)
              nodeCell <- row.cells.find(_.qualifier == ByteString("node"))
              uid = nodeCell.value.utf8String
              metaAddress <- Try { CoreAdapters.deserializeAddress(zookeeper.getData.forPath(s"/servers/$uid")) }.toOption
              node = Node(uid, metaAddress)
            } yield (node, metaTablet)
            res.map(Seq(_)).getOrElse(Seq())
          }
    }
  }

  private def resolveRoot(): Node = {
    val uid = new String(zookeeper.getData.forPath("/root"), StandardCharsets.UTF_8)
    val address = CoreAdapters.deserializeAddress(zookeeper.getData.forPath(s"/server/$uid"))
    Node(uid, address)
  }

  private def resolveMaster(): Node = {
    val uid = new String(zookeeper.getData.forPath("/leader"), StandardCharsets.UTF_8)
    val address = CoreAdapters.deserializeAddress(zookeeper.getData.forPath(s"/server/$uid"))
    Node(uid, address)
  }
}
