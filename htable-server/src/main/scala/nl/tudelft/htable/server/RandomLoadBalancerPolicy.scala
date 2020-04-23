package nl.tudelft.htable.server
import nl.tudelft.htable.core.{Node, Tablet}

import scala.collection.Set
import scala.util.Random

/**
 * A [LoadBalancerPolicy] that selects nodes at random.
 */
class RandomLoadBalancerPolicy extends LoadBalancerPolicy {

  /**
   * The nodes of that are available.
   */
  private var nodes: collection.Seq[Node] = Seq.empty

  override def startCycle(currentAssignments: Map[Node, Set[Tablet]]): Unit = {
    this.nodes = currentAssignments.keys.toSeq
  }

  /**
   * Select the [Node] to which the specified [tablet] should be assigned.
   */
  override def select(tablet: Tablet): Node = nodes(Random.nextInt(nodes.length))

  override def endCycle(): Unit = {
    this.nodes = Seq.empty
  }
}
