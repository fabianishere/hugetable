package nl.tudelft.htable.server.core

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, DispatcherSelector}
import akka.stream.Materializer
import akka.stream.typed.scaladsl.ActorSink
import akka.util.{ByteString, Timeout}
import nl.tudelft.htable.client.HTableInternalClient
import nl.tudelft.htable.core._

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * A load balancer rebalances the unassigned tablets to the active tablet servers.
 */
object LoadBalancerActor {

  /**
   * Commands that are accepted by the [LoadBalancer].
   */
  sealed trait Command

  /**
   * A message to start a load balancing cycle.
   */
  final case class Schedule(nodes: Set[Node]) extends Command

  /**
   * Received when a node reports its tablets.
   */
  private final case class NodeReport(node: Node, tablets: Seq[Tablet]) extends Command

  /**
   * Received when we fail to receive a response from a [NodeManager].
   */
  private final case class NodeFailure(node: Node, failure: Throwable) extends Command

  private case class NodeRow(node: Node, row: Row) extends Command

  private case class NodeComplete(node: Node) extends Command

  /**
   * Construct the behavior for the load balancer.
   *
   * @param zk The reference to the ZooKeeper actor.
   * @param client The client to communicate with the other nodes.
   */
  def apply(zk: ActorRef[ZooKeeperActor.Command], client: HTableInternalClient): Behavior[Command] = Behaviors.setup {
    context =>
      context.log.info("Starting load balancer")
      idle(zk, client)
  }

  /**
   * Construct the behavior for an idle load balancer.
   *
   * @param zk The reference to the ZooKeeper actor.
   * @param client The client to communicate with the other nodes.
   */
  private def idle(zk: ActorRef[ZooKeeperActor.Command], client: HTableInternalClient): Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case Schedule(nodes) => running(zk, client, nodes)
    }

  /**
   * Construct the behavior for a load balancing cycle.
   *
   * @param zk The reference to the ZooKeeper actor.
   * @param client The client to communicate with the other nodes.
   * @param nodes The nodes to load reconstruct over.
   */
  private def running(zk: ActorRef[ZooKeeperActor.Command],
                      client: HTableInternalClient,
                      nodes: Set[Node]): Behavior[Command] = Behaviors.setup { context =>
    // asking someone requires a timeout if the timeout hits without response
    // the ask is failed with a TimeoutException
    implicit val timeout: Timeout = 3.seconds
    implicit val ec: ExecutionContext = context.system.dispatchers.lookup(DispatcherSelector.default())

    context.log.info(s"Starting load balancing cycle ${nodes}")

    nodes.foreach { node =>
      context.pipeToSelf(client.report(node)) {
        case Success(value)     => NodeReport(node, value)
        case Failure(exception) => NodeFailure(node, exception)
      }
    }

    val responses = mutable.HashMap[Node, Seq[Tablet]]()

    Behaviors.receiveMessagePartial {
      case NodeReport(node, tablets) =>
        context.log.info(s"Received pong from $node [$tablets]")
        responses(node) = tablets

        if (responses.size == nodes.size)
          reconstruct(zk, client, responses)
        else
          Behaviors.same
      case NodeFailure(node, _) =>
        responses(node) = Seq.empty
        if (responses.size == nodes.size)
          reconstruct(zk, client, responses)
        else
          Behaviors.same
      case Schedule(nodes) => running(zk, client, nodes)
    }
  }

  /**
   * Construct the behavior for reconstructing the metadata table over the nodes.
   */
  private def reconstruct(zk: ActorRef[ZooKeeperActor.Command],
                          client: HTableInternalClient,
                          tablets: mutable.HashMap[Node, Seq[Tablet]]): Behavior[Command] = Behaviors.setup { context =>
    implicit val timeout: Timeout = 3.seconds
    implicit val mat: Materializer = Materializer(context.system)

    context.log.info("Gathering all known tablets")

    val queuedTablets = mutable.TreeSet[Tablet](Tablet.root) // The tablets to be possibly (re-)assigned
    val assignedTablets = mutable.TreeMap[Tablet, Node]()

    tablets
      .flatMap { case (k, v) => v.map(t => (k, t)) }
      .foreach { case (node, tablet) => assignedTablets(tablet) = node }

    val queriedNodes = mutable.HashSet[Node]()
    val uidToNode = mutable.HashMap[String, Node]()

    tablets.keys.foreach(node => uidToNode(node.uid) = node)

    /**
     * Query the specified [Node] for the metadata table.
     */
    def query(node: Node): Unit = {
      queriedNodes += node
      val sink = ActorSink.actorRef[Command](context.self,
                                             onCompleteMessage = NodeComplete(node),
                                             onFailureMessage = NodeFailure(node, _))
      client
        .read(node, Scan("METADATA", RowRange.unbounded))
        .map(row => NodeRow(node, row))
        .runWith(sink)
    }

    val nodeIterator: Iterator[Node] = Iterator.continually(tablets.keys).flatten

    /**
     * Process the specified queue by querying the relevant nodes for the meta tablet or assigning the tablets and
     * then performing a query.
     */
    def processQueue(queue: mutable.TreeSet[Tablet]): Unit = {
      for (tablet <- queue) {
        val node = assignedTablets.get(tablet) match {
          case Some(value) => value
          case None =>
            val node = nodeIterator.next() // Perform round robin assignment
            // This operation must always return some Seq value
            val assignments = tablets
              .updateWith(node) { tablets =>
                Some(tablets.map(_.appended(tablet)).getOrElse(Seq.empty))
              }
              .get
            context.log.info(s"Assigning tablet $tablet to $node")

            if (Tablet.isRoot(tablet)) {
              zk ! ZooKeeperActor.SetRoot(node)
            }

            client.assign(node, assignments)

            // Update metadata tablets if needed
            val key = ByteString(tablet.table) ++ tablet.range.start
            val metaNode =
              if (Tablet.isRoot(tablet))
                node
              else
                assignedTablets.rangeTo(Tablet("METADATA", RowRange.leftBounded(key))).last._2
            val time = System.currentTimeMillis()
            val mutation = RowMutation("METADATA", key)
              .put(RowCell(ByteString("table"), time, ByteString(tablet.table)))
              .put(RowCell(ByteString("start-key"), time, tablet.range.start))
              .put(RowCell(ByteString("end-key"), time, tablet.range.end))
              .put(RowCell(ByteString("node"), time, ByteString(node.uid)))
              .put(RowCell(ByteString("state"), time, ByteString(TabletState.Served.id)))

            context.log.info(s"Asking $metaNode to update METADATA tablet")
            client.mutate(metaNode, mutation)

            node
        }

        queue -= tablet
        assignedTablets(tablet) = node

        if (Tablet.isMeta(tablet) && !queriedNodes.contains(node)) {
          query(node)
        }
      }
    }

    // Process initial queue
    processQueue(queuedTablets)

    Behaviors.receiveMessagePartial {
      case Schedule(nodes) => running(zk, client, nodes)
      case NodeRow(_, row) =>
        context.log.debug(s"Received row $row")
        for {
          table <- row.cells.find(_.qualifier == ByteString("table"))
          start <- row.cells.find(_.qualifier == ByteString("start-key"))
          end <- row.cells.find(_.qualifier == ByteString("end-key"))
          tablet = Tablet(table.value.utf8String, RowRange(start.value, end.value))
          state <- row.cells.find(_.qualifier == ByteString("state")).map(cell => TabletState(cell.value(0)))
          if state != TabletState.Closed // Do not assign closed tablets
          uid = row.cells.find(_.qualifier == ByteString("node")).map(_.value.utf8String)
        } yield {
          uid.filter(_ => state == TabletState.Served).foreach { cell =>
            // Assign the tablet to the specified node
            uidToNode.get(cell).foreach { node =>
              tablets
                .updateWith(node) { tablets =>
                  Some(tablets.map(_.appended(tablet)).getOrElse(Seq.empty))
                }
                .get
            }
          }

          queuedTablets += tablet
        }
        Behaviors.same
      case NodeComplete(_) =>
        context.log.debug("Received all rows")
        processQueue(queuedTablets)
        if (queuedTablets.isEmpty)
          idle(zk, client)
        else
          Behaviors.same
      case NodeFailure(_, ex) =>
        context.log.error("Load balancer failed", ex)
        idle(zk, client)
    }
  }
}
