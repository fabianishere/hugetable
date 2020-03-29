package nl.tudelft.htable.server.core

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.stream.Materializer
import akka.stream.typed.scaladsl.ActorSink
import akka.util.{ByteString, Timeout}
import nl.tudelft.htable.core.{Node, Row, RowCell, RowMutation, RowRange, Scan, Tablet}
import nl.tudelft.htable.server.core.NodeManager.Mutate

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * A load balancer rebalances the unassigned tablets to the active tablet servers.
 */
object LoadBalancer {

  /**
   * Commands that are accepted by the [LoadBalancer].
   */
  sealed trait Command

  /**
   * A message to start a load balancing cycle.
   */
  final case class Start(nodes: Map[Node, ActorRef[NodeManager.Command]]) extends Command

  /**
   * Received when we receive a response from a [NodeManager].
   */
  private final case class NodeEvent(node: Node, event: NodeManager.Event) extends Command

  /**
   * Received when we fail to receive a response from a [NodeManager].
   */
  private final case class NodeFailure(node: Node, failure: Throwable) extends Command

  private case class NodeRow(node: Node, row: Row) extends Command
  private case class NodeComplete(node: Node) extends Command

  /**
   * Construct the behavior for the load balancer.
   */
  def apply(): Behavior[Command] = Behaviors.setup { context =>
    context.log.info("Starting load balancer")
    idle()
  }

  /**
   * Construct the behavior for an idle load balancer.
   */
  private def idle(): Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case Start(nodes) => running(nodes)
    }

  /**
   * Construct the behavior for a load balancing cycle.
   *
   * @param nodes The nodes to load reconstruct over.
   */
  private def running(nodes: Map[Node, ActorRef[NodeManager.Command]]): Behavior[Command] = Behaviors.setup { context =>
    // asking someone requires a timeout if the timeout hits without response
    // the ask is failed with a TimeoutException
    implicit val timeout: Timeout = 3.seconds

    context.log.info("Starting load balancing cycle")

    nodes.foreach {
      case (node, ref) =>
        context.ask(ref, NodeManager.QueryTablets) {
          case Success(event: NodeManager.Event) => NodeEvent(node, event)
          case Failure(e)                        => NodeFailure(node, e)
        }
    }

    val responses = mutable.HashMap[Node, Seq[Tablet]]()

    Behaviors.receiveMessagePartial {
      case NodeEvent(_, NodeManager.QueryTabletsResponse(node, tablets)) =>
        context.log.info(s"Received pong from $node [$tablets]")
        responses(node) = tablets

        if (responses.size == nodes.size)
          reconstruct(nodes, responses)
        else
          Behaviors.same
      case NodeFailure(node, _) =>
        responses(node) = Seq.empty
        if (responses.size == nodes.size)
          reconstruct(nodes, responses)
        else
          Behaviors.same
      case Start(nodes) => running(nodes)
    }
  }

  /**
   * Construct the behavior for reconstructing the metadata table over the nodes.
   */
  private def reconstruct(nodes: Map[Node, ActorRef[NodeManager.Command]],
                  nodeTablets: mutable.HashMap[Node, Seq[Tablet]]): Behavior[Command] = Behaviors.setup { context =>
    implicit val timeout: Timeout = 3.seconds
    implicit val mat: Materializer = Materializer(context.system)

    context.log.info("Gathering all known tablets")

    val queuedTablets = mutable.TreeSet[Tablet](Tablet.root) // The tablets to be possibly (re-)assigned
    val assignedTablets = mutable.TreeMap[Tablet, Node]()

    nodeTablets
      .flatMap { case (k, v) => v.map(t => (k, t)) }
      .foreach { case (node, tablet) => assignedTablets(tablet) = node }

    val queriedNodes = mutable.HashSet[Node]()
    val uidToNode = mutable.HashMap[String, Node]()

    nodes.keys.foreach(node => uidToNode(node.uid) = node)

    /**
     * Query the specified [Node] for the metadata table.
     */
    def query(node: Node): Unit = {
      queriedNodes += node
      context.ask(nodes(node), (ref: ActorRef[NodeManager.ReadResponse]) =>
        NodeManager.Read(Scan("METADATA", RowRange.unbounded), ref)
      ) {
        case Success(event) => NodeEvent(node, event)
        case Failure(e)     => NodeFailure(node, e)
      }
    }

    val nodeIterator: Iterator[(Node, ActorRef[NodeManager.Command])] = Iterator.continually(nodes).flatten

    /**
     * Process the specified queue by querying the relevant nodes for the meta tablet or assigning the tablets and
     * then performing a query.
     */
    def processQueue(queue: mutable.TreeSet[Tablet]): Unit = {
      for (tablet <- queue) {
        val node = assignedTablets.get(tablet) match {
          case Some(value) => value
          case None =>
            val (node, ref) = nodeIterator.next() // Perform round robin assignment
            // This operation must always return some Seq value
            val assignments = nodeTablets.updateWith(node) { tablets => Some(tablets.map(_.appended(tablet)).getOrElse(Seq.empty)) }.get
            context.log.info(s"Assigning tablet $tablet to $node")
            ref ! NodeManager.Assign(assignments)
            node
        }

        assignedTablets(tablet) = node
        queue -= tablet

        // Update metadata tablets if needed
        if (!Tablet.isRoot(tablet)) {
          val key = ByteString(tablet.table) ++ tablet.range.start
          val (_, metaNode) = assignedTablets.rangeTo(Tablet("METADATA", RowRange.leftBounded(key))).last
          val metaRef = nodes(metaNode)
          val time = System.currentTimeMillis()
          val mutation = RowMutation("METADATA", key)
              .append(RowCell(ByteString("tablet"), time, ByteString(tablet.table)))
              .append(RowCell(ByteString("start-key"), time, tablet.range.start))
              .append(RowCell(ByteString("end-key"), time, tablet.range.end))
              .append(RowCell(ByteString("node"), time, ByteString(node.uid)))

          context.log.info(s"Asking $metaNode to update METADATA tablet")

          context.ask(metaRef, (ref: ActorRef[NodeManager.MutateResponse.type]) => Mutate(mutation, ref)) {
            case Success(event) => NodeEvent(node, event)
            case Failure(e)     => NodeFailure(node, e)
          }
        }

        if (Tablet.isMeta(tablet) && !queriedNodes.contains(node)) {
          query(node)
        }
      }
    }

    // Process initial queue
    processQueue(queuedTablets)

    Behaviors.receiveMessagePartial {
      case Start(nodes) => running(nodes)
      case NodeEvent(node, NodeManager.ReadResponse(rows)) =>
        val sink = ActorSink.actorRef[Command](context.self,
                                               onCompleteMessage = NodeComplete(node),
                                               onFailureMessage = NodeFailure(node, _))
        rows.map(row => NodeRow(node, row)).runWith(sink)
        Behaviors.same
      case NodeEvent(_, event) =>
        context.log.info(s"Received unknown event $event")
        Behaviors.same
      case NodeRow(_, row) =>
        context.log.info(s"Received row $row")
        for {
          table <- row.cells.find(_.qualifier == ByteString("table"))
          start <- row.cells.find(_.qualifier == ByteString("start-key"))
          end <- row.cells.find(_.qualifier == ByteString("end-key"))
          uid <- row.cells.find(_.qualifier == ByteString("node"))
          tablet = Tablet(table.value.utf8String, RowRange(start.value, end.value))
        } yield {
          // Assign the tablet to the specified node
          uidToNode.get(uid.value.utf8String).foreach { node =>
            nodeTablets.updateWith(node) { tablets => Some(tablets.map(_.appended(tablet)).getOrElse(Seq.empty)) }.get
          }

          queuedTablets += tablet
        }
        Behaviors.same
      case NodeComplete(_) =>
        context.log.info("Received all rows")
        processQueue(queuedTablets)
        if (queuedTablets.isEmpty)
          idle()
        else
          Behaviors.same
      case NodeFailure(_, ex) =>
        context.log.error("Load balancer failed", ex)
        idle()
    }
  }

}
