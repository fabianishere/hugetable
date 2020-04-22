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
      // We append a null-byte to the end of the range to make it inclusive
      case Get(_, key)           => resolve(Tablet(query.table, RowRange(key, key ++ ByteString(0))))
      case Scan(table, range, reversed) =>
        val tablets = resolve(Tablet(table, range))
        // Make sure we reverse the tablets if our scan is reversed
        if (reversed)
          tablets
            .fold(List.empty[(Node, Tablet)])((acc: List[(Node, Tablet)], curr: (Node, Tablet)) => curr :: acc) // Reverse
            .mapConcat[(Node, Tablet)](identity)
        else
          tablets
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

  override def assign(node: Node, tablets: Set[Tablet]): Future[Done] = {
    resolver.openInternal(node).assign(AssignRequest(tablets.toSeq)).map(_ => Done)
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
    val metaTablet =
      if (tablet.table.equalsIgnoreCase("METADATA"))
        tablet
      else
        Tablet("METADATA", RowRange(ByteString(tablet.table) ++ tablet.range.start,
          ByteString(tablet.table) ++ tablet.range.end))

    val meta = scanMeta(rootClient, metaTablet)

    // In case we are looking for rows in the METADATA table, we have already found our target tablets.
    if (tablet.table.equalsIgnoreCase("METADATA")) {
      return meta
    }

    meta
      .via(new RequireOne(() => new IllegalArgumentException(s"Unknown table ${tablet.table}")))
      .flatMapConcat {
        case (metaNode, metaTablet) =>
          val metaClient = resolver.openClient(metaNode)

          val range =
            if (tablet.range.isRightBounded) {
              var right = tablet.range.end
              // Take into account the maximum row of the META tablet
              if (metaTablet.range.isRightBounded) {
                val metaRight = metaTablet.range.end.drop(tablet.table.length)
                right = Order.keyOrdering.min(metaRight, right)
              }
              RowRange.rightBounded(right)
            } else {
              RowRange.unbounded
            }

          scanMeta(metaClient, Tablet(tablet.table, range))
      }
  }

  /**
   * Scan the entries of a METADATA tablet.
   *
   * @param client The client to scan the tablet of.
   * @param tablet The particular tablet to look for on this client.
   * @return A source containing the METADATA rows of interest.
   */
  private def scanMeta(client: ClientServiceClient, tablet: Tablet): Source[(Node, Tablet), NotUsed]  = {
    // We append a null-byte to the end of the range to make it inclusive
    val range =
      if (tablet.range.isUnbounded || !tablet.range.isRightBounded) {
        RowRange.prefix(ByteString(tablet.table))
      } else {
        val left = ByteString(tablet.table)
        val right = ByteString(tablet.table) ++ tablet.range.end ++ ByteString(0)
        RowRange(left, right)
      }

    // Our resolve procedure works as follows:
    // 1. Scan the metadata table from the end-key until the first matching start row occurs
    // 2. Verify whether this row is still part of the table we are looking for
    // 3. Reverse the rows
    // 5. Convert the METADATA rows into a stream of nodes and tablets
    read(Scan("METADATA", range, reversed = true), client)
      // Take rows up until we find the first row which is outside our range (this is the first tablet)
      .takeWhile(row => Order.keyOrdering.gteq(row.key, tablet.range.start), inclusive = true)
      // Reverse the order of the cells
      .fold(List.empty[Row])((acc: List[Row], curr: Row) => curr :: acc) // Reverse
      .mapConcat[Row](identity)
      .mapConcat[(Node, Tablet)](row => parseMeta(row).map(Seq(_)).getOrElse(Seq()))
  }

  /**
   * Convert a [Row] into the [Node] that is hosting some [Tablet].
   */
  private def parseMeta(row: Row): Option[(Node, Tablet)] = {
    for {
      (tablet, state, uid) <- MetaHelpers.readRow(row)
      if state == TabletState.Served
      uid <- uid
      metaAddress <- Try {
        CoreAdapters.deserializeAddress(zookeeper.getData.forPath(s"/servers/$uid"))
      }.toOption
      targetNode = Node(uid, metaAddress)
    } yield (targetNode, tablet)
  }

  /**
   * Resolve the address to the root tablet in ZooKeeper.
   */
  private def resolveRoot(): Node = {
    val uid = new String(zookeeper.getData.forPath("/root"), StandardCharsets.UTF_8)
    val address = CoreAdapters.deserializeAddress(zookeeper.getData.forPath(s"/servers/$uid"))
    Node(uid, address)
  }

  /**
   * Resolve the address to the master node in ZooKeeper.
   */
  private def resolveMaster(): Node = {
    val leader = zookeeper.getChildren.forPath("/leader").asScala.min(lockOrdering)
    val uid = new String(zookeeper.getData.forPath(s"/leader/$leader"), StandardCharsets.UTF_8)
    val address = CoreAdapters.deserializeAddress(zookeeper.getData.forPath(s"/servers/$uid"))
    Node(uid, address)
  }

  /**
   * Ordering that is used by Curator LeaderLatch recipe.
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
