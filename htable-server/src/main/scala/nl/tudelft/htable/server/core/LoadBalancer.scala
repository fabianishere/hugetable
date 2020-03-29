package nl.tudelft.htable.server.core

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.stream.Materializer
import akka.stream.typed.scaladsl.ActorSink
import akka.util.{ByteString, Timeout}
import nl.tudelft.htable.core.{Node, Row, RowRange, Scan, Tablet}

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
  def idle(): Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case Start(nodes) => running(nodes)
    }

  /**
   * Construct the behavior for a load balancing cycle.
   *
   * @param nodes The nodes to load reconstruct over.
   */
  def running(nodes: Map[Node, ActorRef[NodeManager.Command]]): Behavior[Command] = Behaviors.setup { context =>
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
          reconstruct(nodes, responses.toMap)
        else
          Behaviors.same
      case NodeFailure(node, _) =>
        responses(node) = Seq.empty
        if (responses.size == nodes.size)
          reconstruct(nodes, responses.toMap)
        else
          Behaviors.same
      case Start(nodes) => running(nodes)
    }
  }

  /**
   * Construct the behavior for reconstructing the metadata table over the nodes.
   */
  def reconstruct(nodes: Map[Node, ActorRef[NodeManager.Command]],
                  responses: Map[Node, Seq[Tablet]]): Behavior[Command] = Behaviors.setup { context =>
    implicit val timeout: Timeout = 3.seconds
    implicit val mat: Materializer = Materializer(context.system)

    context.log.info("Gathering tablets")

    val allTablets = mutable.TreeSet[Tablet]()
    val tablets = mutable.TreeMap[Tablet, Node]()

    responses
      .flatMap { case (k, v) => v.map(t => (k, t)) }
      .foreach {
        case (node, tablet) =>
          tablets(tablet) = node
      }

    /**
     * Query the specified [Node] for the metadata table.
     */
    def query(node: Node): Unit = {
      context.ask(nodes(node),
                  (ref: ActorRef[NodeManager.ReadResponse]) =>
                    NodeManager.Read(Scan("METADATA", RowRange(ByteString.empty, ByteString.empty)), ref)) {
        case Success(event) => NodeEvent(node, event)
        case Failure(e)     => NodeFailure(node, e)
      }
    }

    val root = tablets.get(Tablet.root) match {
      case Some(value) => value
      case None =>
        val (node, ref) = nodes.head
        context.log.info(s"Assigning root to $node")
        ref ! NodeManager.Assign(Seq(Tablet.root))
        node
    }

    query(root)

    Behaviors.receiveMessagePartial {
      case Start(nodes) => running(nodes)
      case NodeEvent(node, NodeManager.ReadResponse(rows)) =>
        val sink = ActorSink.actorRef[Command](context.self,
                                               onCompleteMessage = NodeComplete(node),
                                               onFailureMessage = NodeFailure(node, _))
        rows.map(row => NodeRow(node, row)).runWith(sink)
        Behaviors.same
      case NodeRow(_, row) =>
        val table = row.cells.find(_.qualifier == ByteString("table")).head.value.utf8String
        val startKey = row.cells.find(_.qualifier == ByteString("start-key")).head.value
        val tablet = Tablet(table, RowRange.leftBounded(startKey))

        allTablets += tablet
        Behaviors.same
      case NodeComplete(_) => redistribute(nodes, root, allTablets)
      case NodeFailure(_, ex) =>
        context.log.error("Load balancer failed", ex)
        idle()
    }
  }

  /**
   * Construct the behavior for distributing the tablets over the nodes.
   */
  def redistribute(nodes: Map[Node, ActorRef[NodeManager.Command]],
                   root: Node,
                   tablets: mutable.TreeSet[Tablet]): Behavior[Command] =
    Behaviors.setup { context =>
      context.log.info(s"Redistributing ${tablets.size} remaining tablets")

      tablets.grouped(nodes.size).zip(nodes).foreach {
        case (sub, (node, ref)) =>
          val assignment = if (node == root)(sub.toSeq.+:(Tablet.root)) else sub.toSeq
          context.log.info(s"Assigning ${assignment} to $node")
          ref ! NodeManager.Assign(assignment)
      }

      idle()
    }

}
