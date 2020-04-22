package nl.tudelft.htable.client

import akka.Done
import nl.tudelft.htable.core.{Node, Tablet}

import scala.concurrent.Future

/**
 * A client that provides access to the internal service of the nodes.
 */
private[htable] trait HTableInternalClient extends HTableClient {

  /**
   * Obtain the master node of the cluster.
   */
  def master: Node

  /**
   * Ping the specified node.
   *
   * @param node The node to ping.
   */
  def ping(node: Node): Future[Done]

  /**
   * Query the specified node for the tablet it is serving.
   *
   * @param node The node to query.
   */
  def report(node: Node): Future[Seq[Tablet]]

  /**
   * Assign the specified tablets to the node.
   *
   * @param node The node to assign the tablets to.
   * @param tablets The tablets to assign to the node.
   */
  def assign(node: Node, tablets: Set[Tablet]): Future[Done]
}
