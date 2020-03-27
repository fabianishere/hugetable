package nl.tudelft.htable.server.core


import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, DispatcherSelector}
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import nl.tudelft.htable.core.Node

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
 * A load balancer rebalances the unassigned tablets to the active tablet servers.
 */
object LoadBalancer {

  /**
   * Commands that are accepted by the [LoadBalancer].
   */
  sealed trait Command

  /**
   * Received when we receive a response from a [NodeManager].
   */
  private final case class NodeEvent(node: Node, event: NodeManager.Event) extends Command

  /**
   * Received when we fail to receive a response from a [NodeManager].
   */
  private final case class NodeFailure(node: Node, failure: Throwable) extends Command

  /**
   * Construct the behavior for the load balancer.
   */
  def apply(nodes: Map[Node, ActorRef[NodeManager.Command]]): Behavior[Command] = Behaviors.setup { context =>
    // asking someone requires a timeout if the timeout hits without response
    // the ask is failed with a TimeoutException
    implicit val timeout: Timeout = 3.seconds

    context.log.info("Starting load balancer")

    nodes.foreach {
      case (node, ref) => context.ask(ref, NodeManager.Ping) {
        case Success(event: NodeManager.Event) => NodeEvent(node, event)
        case Failure(e) => NodeFailure(node, e)
      }
    }

    Behaviors.receiveMessagePartial {
      case NodeEvent(_, NodeManager.Pong(member)) =>
        context.log.info(s"Received pong from $member")
        Behaviors.same
    }
  }
}
