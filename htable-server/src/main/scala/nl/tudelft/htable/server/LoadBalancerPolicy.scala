package nl.tudelft.htable.server

import nl.tudelft.htable.core.{Node, Tablet}

import scala.collection.Set

/**
 * A load balancing policy decides on which node a tablet should be placed.
 */
trait LoadBalancerPolicy {

  /**
   * This method is invoked when the load balancing cycle is started.
   *
   * @param currentAssignments The current assignments.
   */
  def startCycle(currentAssignments: Map[Node, Set[Tablet]]): Unit

  /**
   * Select the [Node] to which the specified [tablet] should be assigned.
   */
  def select(tablet: Tablet): Node

  /**
   * This method is invoked when the load balancing cycle has ended.
   */
  def endCycle(): Unit
}
