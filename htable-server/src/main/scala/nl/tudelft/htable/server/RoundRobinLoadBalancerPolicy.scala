package nl.tudelft.htable.server
import nl.tudelft.htable.core.{Node, Tablet}

import scala.collection.Set

/**
 * A [LoadBalancerPolicy] that selects nodes round robin.
 */
class RoundRobinLoadBalancerPolicy extends LoadBalancerPolicy {

  /**
   * The nodes from which we can pick.
   */
  private var nodes: Iterator[Node] = Iterator.empty

  override def startCycle(currentAssignments: Map[Node, Set[Tablet]]): Unit = {
    this.nodes = Iterator.continually(currentAssignments.keys).flatten
  }

  override def select(tablet: Tablet): Node = nodes.next()

  override def endCycle(): Unit = {
    this.nodes = Iterator.empty
  }
}
