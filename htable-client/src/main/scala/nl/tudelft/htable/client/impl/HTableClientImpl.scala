package nl.tudelft.htable.client.impl

import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import akka.{Done, NotUsed}
import nl.tudelft.htable.client.{HTableInternalClient, ServiceResolver}
import nl.tudelft.htable.core._
import nl.tudelft.htable.protocol.CoreAdapters
import nl.tudelft.htable.protocol.admin.{CreateTableRequest, DeleteTableRequest, InvalidateRequest}
import nl.tudelft.htable.protocol.client.{ClientServiceClient, MutateRequest, ReadRequest}
import nl.tudelft.htable.protocol.internal.{AssignRequest, PingRequest, ReportRequest, SplitRequest}
import org.apache.curator.framework.CuratorFramework

import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Internal implementation of the [HTableClient] trait.
 *
 * @param zookeeper The ZooKeeper client used to connect to the cluster.
 * @param actorSystem The actor system to drive the client.
 * @param resolver The resolver used to connect to the nodes.
 */
private[client] class HTableClientImpl(private val zookeeper: CuratorFramework,
                                       private val actorSystem: ActorSystem,
                                       private val resolver: ServiceResolver)
    extends HTableInternalClient {
  implicit val sys: ActorSystem = actorSystem
  implicit val mat: Materializer = Materializer(sys)
  implicit val ec: ExecutionContextExecutor = sys.dispatcher
  private val promise = Promise[Done]()

  override def master: Node = resolveMaster()

  override def invalidate(tablets: Seq[Tablet]): Future[Done] = {
    val node = resolveMaster()
    val client = resolver.openAdmin(node)
    client.invalidate(InvalidateRequest(tablets)).map(_ => Done)
  }

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

  override def split(tablet: Tablet, splitKey: ByteString): Future[Done] = {
    resolve(tablet)
      .via(new RequireOne(() => new IllegalArgumentException(s"Table ${tablet.table} not found")))
      .flatMapConcat {
        case (node, _) =>
          Source.future(
            resolver
              .openInternal(node)
              .split(SplitRequest(Some(tablet), splitKey)))
      }
      .map(_ => Done)
      .runWith(Sink.head)
  }

  override def read(node: Node, query: Query): Source[Row, NotUsed] = {
    val client = resolver.openClient(node)
    read(query, client)
  }

  override def read(query: Query): Source[Row, NotUsed] = {
    val nodes = query match {
      case Get(_, key)           => resolve(Tablet(query.table, RowRange(key, key ++ ByteString(9, 9))))
      case Scan(table, range, _) => resolve(Tablet(table, range))
    }

    nodes
      .via(new RequireOne(() => new IllegalArgumentException(s"Table ${query.table} not found")))
      .flatMapConcat {
        case (node, tablet) =>
          query match {
            case Get(table, key) =>
              read(node, query)
                .via(new RequireOne(() =>
                  new IllegalArgumentException(s"Unknown key '${key.utf8String}' in table ${table}")))
            case Scan(table, range, reversed) =>
              val reducedStart = Order.keyOrdering.max(range.start, tablet.range.start)
              val reducedEnd =
                if (range.isRightBounded && tablet.range.isRightBounded)
                  Order.keyOrdering.min(range.end, tablet.range.end)
                else
                  Order.keyOrdering.max(range.end, tablet.range.end)
              read(node, Scan(table, RowRange(reducedStart, reducedEnd), reversed))
          }
      }
  }

  private def read(query: Query, client: ClientServiceClient): Source[Row, NotUsed] = {
    client
      .read(ReadRequest(Some(query)))
      .mapConcat(_.rows)
  }

  override def mutate(node: Node, mutation: RowMutation): Future[Done] = {
    val client = resolver.openClient(node)
    client.mutate(MutateRequest(Some(mutation))).map(_ => Done)
  }

  override def mutate(mutation: RowMutation): Future[Done] = {
    resolve(Tablet(mutation.table, RowRange(mutation.key, mutation.key ++ ByteString(9))))
      .via(new RequireOne(() => new IllegalArgumentException(s"Table ${mutation.table} not found")))
      .flatMapConcat {
        case (node, _) => Source.future(mutate(node, mutation))
      }
      .runForeach(_ => ())
  }

  override def ping(node: Node): Future[Done] = {
    resolver.openInternal(node).ping(PingRequest()).map(_ => Done)
  }

  override def report(node: Node): Future[Seq[Tablet]] = {
    resolver.openInternal(node).report(ReportRequest()).map(_.tablets)
  }

  override def assign(node: Node, tablets: Seq[Tablet]): Future[Done] = {
    resolver.openInternal(node).assign(AssignRequest(tablets)).map(_ => Done)
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

    // We append a null-byte to the end of the range to make it inclusive
    val metaKey =
      if (tablet.table.equalsIgnoreCase("METADATA"))
        ByteString("METADATA") ++ tablet.range.end ++ ByteString(0)
      else
        ByteString("METADATA" ++ tablet.table) ++ tablet.range.end ++ ByteString(0)

    // Our resolve procedure works as follows:
    // 1. Scan the metadata table from the end-key until the first matching start row occurs
    // 2. Verify whether this row is still part of the table we are looking for
    // 3. Reverse the rows
    // 5. Convert the METADATA rows into a stream of nodes and tablets
    val meta: Source[(Node, Tablet), NotUsed] =
      read(Scan("METADATA", RowRange.rightBounded(metaKey), reversed = true), rootClient)
        .takeWhile(row => Order.keyOrdering.gteq(row.key, tablet.range.start), inclusive = true)
        .takeWhile(row =>
          row.cells.find(_.qualifier == ByteString("table")).map(_.value.utf8String).contains("METADATA"))
        .fold(List.empty[Row])((acc: List[Row], curr: Row) => curr :: acc) // Reverse
        .mapConcat[Row](identity)
        .mapConcat[(Node, Tablet)] { row =>
          val res = parseMeta(row)
          res.map(Seq(_)).getOrElse(Seq())
        }

    // In case we are looking for rows in the METADATA table, we have already found our target tablets.
    if (tablet.table.equalsIgnoreCase("METADATA")) {
      return meta
    }

    meta
      .via(new RequireOne(() => new IllegalArgumentException(s"Unknown table ${tablet.table}")))
      .flatMapConcat {
        case (metaNode, metaTablet) =>
          val metaClient = resolver.openClient(metaNode)
          var prefixRange = RowRange.prefix(ByteString(tablet.table))
          if (metaTablet.range.isRightBounded) {
            prefixRange = RowRange(prefixRange.start, Order.keyOrdering.min(prefixRange.end, metaTablet.range.end))
          }
          read(Scan("METADATA", prefixRange), metaClient)
            .dropWhile(
              row =>
                row.cells
                  .find(_.qualifier == ByteString("end-key"))
                  .exists(cell =>
                    if (cell.value.isEmpty) false else Order.keyOrdering.lteq(cell.value, tablet.range.start)))
            .takeWhile(row => // Take rows until we find one of a different table
              row.cells.find(_.qualifier == ByteString("table")).map(_.value.utf8String).contains(tablet.table))
            .mapConcat[(Node, Tablet)] { row =>
              val res = parseMeta(row)
              res.map(Seq(_)).getOrElse(Seq())
            }
      }
  }

  private def parseMeta(row: Row): Option[(Node, Tablet)] = {
    for {
      startKey <- row.cells.find(_.qualifier == ByteString("start-key"))
      endKey <- row.cells.find(_.qualifier == ByteString("end-key"))
      table <- row.cells.find(_.qualifier == ByteString("table"))
      range = RowRange(startKey.value, endKey.value)
      targetTablet = Tablet(table.value.utf8String, range)
      state <- row.cells.find(_.qualifier == ByteString("state")).map(cell => TabletState(cell.value(0)))
      if state == TabletState.Served
      nodeCell <- row.cells.find(_.qualifier == ByteString("node"))
      uid = nodeCell.value.utf8String
      metaAddress <- Try {
        CoreAdapters.deserializeAddress(zookeeper.getData.forPath(s"/servers/$uid"))
      }.toOption
      targetNode = Node(uid, metaAddress)
    } yield (targetNode, targetTablet)
  }

  private def resolveRoot(): Node = {
    val uid = new String(zookeeper.getData.forPath("/root"), StandardCharsets.UTF_8)
    val address = CoreAdapters.deserializeAddress(zookeeper.getData.forPath(s"/servers/$uid"))
    Node(uid, address)
  }

  private def resolveMaster(): Node = {
    val leader = zookeeper.getChildren.forPath("/leader").asScala.min(lockOrdering)
    val uid = new String(zookeeper.getData.forPath(s"/leader/$leader"), StandardCharsets.UTF_8)
    val address = CoreAdapters.deserializeAddress(zookeeper.getData.forPath(s"/servers/$uid"))
    Node(uid, address)
  }

  /**
   * Ordering for ZooKeeper locks.
   */
  private val lockOrdering: Ordering[String] = new Ordering[String] {
    private val lockName = "latch-"

    private def fix(str: String): String = {
      var index = str.lastIndexOf(lockName)
      if (index >= 0) {
        index += lockName.length
        return if (index <= str.length) str.substring(index)
        else ""
      }
      str
    }

    override def compare(x: String, y: String): Int = {
      fix(x).compareTo(fix(y))
    }
  }
}
