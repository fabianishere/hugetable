package nl.tudelft.htable.server

import akka.Done
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, DispatcherSelector}
import akka.stream.Materializer
import akka.stream.typed.scaladsl.ActorSink
import akka.util.{ByteString, ByteStringBuilder, Timeout}
import nl.tudelft.htable.client.HTableInternalClient
import nl.tudelft.htable.client.impl.MetaHelpers
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
  private final case class NodeReport(node: Node, tablets: Set[Tablet]) extends Command

  /**
   * Received when we fail to receive a response from a [NodeManager].
   */
  private final case class NodeFailure(node: Node, failure: Throwable) extends Command

  private case class NodeRow(node: Node, row: Row) extends Command

  private case class NodeComplete(node: Node) extends Command

  /**
   * Construct the behavior for the load balancer.
   *
   * @param client The client to communicate with the other nodes.
   * @param policy The load balancing policy.
   */
  def apply(client: HTableInternalClient,
            policy: LoadBalancerPolicy): Behavior[Command] = Behaviors.setup { context =>
    context.log.info("Starting load balancer")
    idle(client, policy)
  }

  /**
   * Construct the behavior for an idle load balancer.
   *
   * @param client The client to communicate with the other nodes.
   * @param policy The load balancing policy.
   */
  private def idle(client: HTableInternalClient,
                   policy: LoadBalancerPolicy): Behavior[Command] =
    Behaviors.receiveMessagePartial {
      case Schedule(nodes) => running(client, policy, nodes)
    }

  /**
   * Construct the behavior for a load balancing cycle.
   *
   * @param client The client to communicate with the other nodes.
   * @param policy The load balancing policy.
   * @param nodes The nodes to load reconstruct over.
   */
  private def running(client: HTableInternalClient,
                      policy: LoadBalancerPolicy,
                      nodes: Set[Node]): Behavior[Command] = Behaviors.setup { context =>
    // asking someone requires a timeout if the timeout hits without response
    // the ask is failed with a TimeoutException
    implicit val timeout: Timeout = 3.seconds
    implicit val ec: ExecutionContext = context.system.dispatchers.lookup(DispatcherSelector.default())

    context.log.info(s"Starting load balancing cycle ${nodes}")

    nodes.foreach { node =>
      context.pipeToSelf(client.report(node)) {
        case Success(value)     => NodeReport(node, value.toSet)
        case Failure(exception) => NodeFailure(node, exception)
      }
    }

    val responses = mutable.HashMap[Node, Set[Tablet]]()

    Behaviors.receiveMessagePartial {
      case NodeReport(node, tablets) =>
        context.log.debug(s"Received pong from $node [$tablets]")
        responses(node) = tablets

        if (responses.size == nodes.size)
          reconstruct(client, policy, responses)
        else
          Behaviors.same
      case NodeFailure(node, _) =>
        responses(node) = Set.empty
        if (responses.size == nodes.size)
          reconstruct(client, policy, responses)
        else
          Behaviors.same
      case Schedule(nodes) => running(client, policy, nodes)
    }
  }

  /**
   * Construct the behavior for reconstructing the metadata table over the nodes.
   */
  private def reconstruct(client: HTableInternalClient,
                          policy: LoadBalancerPolicy,
                          tablets: mutable.HashMap[Node, Set[Tablet]]): Behavior[Command] = Behaviors.setup { context =>
    implicit val timeout: Timeout = 3.seconds
    implicit val ec: ExecutionContext = context.system.dispatchers.lookup(DispatcherSelector.blocking())
    implicit val mat: Materializer = Materializer(context.system)

    context.log.debug("Gathering all known tablets")

    val queuedTablets = mutable.TreeSet[Tablet]() // The tablets to be possibly (re-)assigned
    val queriedNodes = mutable.HashSet[Node]() // Nodes that have been queried
    val newAssignment = mutable.TreeMap[Tablet, Node]()

    // Map all nodes to their unique identifiers
    val uidToNode = tablets.keys.map(node => (node.uid, node)).toMap

    // Start load balancing cycle
    policy.startCycle(tablets.keySet)

    // Make sure the root tablet is up and if not, select a node to host the tablet.
    val rootNode = tablets.find(_._2.exists(Tablet.isRoot)) match {
      case Some((node, _)) => node
      case None =>
        val selectedNode = policy.select(Tablet.root)
        context.log.debug(s"Assigning tablet root to $selectedNode")
        // Make sure we also assign the tablet
        val nodeAssignment = tablets.getOrElse(selectedNode, Set.empty) + Tablet.root
        client.assign(selectedNode, nodeAssignment)
        selectedNode
    }

    // Clear the current assignments
    tablets.clear()
    tablets(rootNode) = Set(Tablet.root)

    // Query the root node
    query(rootNode)

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

    /**
     * Assign the specified [tablet] to the specified [node].
     */
    def assign(tablet: Tablet, node: Node, shouldUpdateMeta: Boolean = true, isNew: Boolean = false): Unit = {
      // Note that this operation must always return some Seq value
      val assignments = tablets
        .updateWith(node) { tablets =>
          Some(tablets.map(_ + tablet).getOrElse(Set.empty))
        }
        .get

      // Update metadata tablet to reflect the assignment
      val key = ByteString(tablet.table) ++ tablet.range.start
      val mutation =
        if (isNew)
          MetaHelpers.writeNew(tablet, TabletState.Served, Some(node))
        else
          MetaHelpers.writeExisting(tablet, TabletState.Served, Some(node))

      val metaNode =
        if (Tablet.isRoot(tablet))
          node
        else
          newAssignment.rangeTo(Tablet("METADATA", RowRange.leftBounded(key))).last._2


      context.log.debug(s"Assigning $tablet to $node [$assignments]")
      // Assign the tablet to the chosen node
      client.assign(node, assignments)
        .andThen { _ =>
          if (shouldUpdateMeta) {
            context.log.debug(s"Asking $metaNode to update METADATA tablet")
            client.mutate(metaNode, mutation)
          } else {
            Some(Done)
          }
        }
        .onComplete {
          case Failure(exception) =>
            context.log.error("Failed to assign tablet to node", exception)
          case Success(res) =>
            context.log.debug(s"Successfully assigned $tablet to $node: $res")
        }
    }

    /**
     * Process the specified queue by querying the relevant nodes for the meta tablet or assigning the tablets and
     * then performing a query.
     */
    def processQueue(queue: mutable.TreeSet[Tablet]): Unit = {
      for (tablet <- queue) {
        val node = newAssignment.get(tablet) match {
          case Some(value) => value
          case None =>
            val node = policy.select(tablet)

            assign(tablet, node)
            newAssignment(tablet) = node
            node
        }

        queue -= tablet

        if (Tablet.isMeta(tablet) && !queriedNodes.contains(node)) {
          query(node)
        }
      }
    }

    Behaviors.receiveMessagePartial {
      case Schedule(nodes) => running(client, policy, nodes)
      case NodeRow(_, row) =>
        context.log.debug(s"Received row $row")
        MetaHelpers.readRow(row) match {
          case Some((tablet, state, uid)) =>
            context.log.debug(s"Found $tablet $state $uid")

            // Update the assignments
            uid.flatMap(uidToNode.get).foreach { node =>
              tablets
                .updateWith(node) { tablets => Some(tablets.map(_ + tablet).getOrElse(Set.empty)) }
            }
            if (state != TabletState.Closed) {
              queuedTablets += tablet
            }
          case None =>
            context.log.error(s"Failed to parse meta row $row")
        }
        Behaviors.same
      case NodeComplete(node) =>
        if (queuedTablets.isEmpty && node == rootNode) {
          context.log.info(s"$node tablet is empty. Initializing METADATA")
          // If the root node does not contain any rows, it means that the METADATA table is not populated. Therefore,
          // we initialize it here.
          assign(Tablet.root, rootNode, isNew = true)
          idle(client, policy)
        } else {
          context.log.debug(s"Queued ${queuedTablets.size} METADATA rows")
          processQueue(queuedTablets)

          if (queuedTablets.isEmpty) {
            policy.endCycle()
            idle(client, policy)
          } else {
            Behaviors.same
          }
        }
      case NodeFailure(_, ex) =>
        context.log.error("Load balancer failed", ex)
        policy.endCycle()
        idle(client, policy)
    }
  }
}
