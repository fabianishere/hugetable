package nl.tudelft.htable.server.core

/**
 * A load balancer rebalances the unassigned tablets to the active tablet servers.
 */
object LoadBalancer {

  /**
   * Commands that are accepted by the [LoadBalancer].
   */
  sealed trait Command
}
