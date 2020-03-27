package nl.tudelft.htable.server.core

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

/**
 * A load balancer rebalances the unassigned tablets to the active tablet servers.
 */
object LoadBalancer {

  /**
   * Commands that are accepted by the [LoadBalancer].
   */
  sealed trait Command

  /**
   * Construct the behavior for the load balancer.
   */
  def apply(): Behavior[Command] = Behaviors.empty
}
