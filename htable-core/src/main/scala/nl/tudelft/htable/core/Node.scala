package nl.tudelft.htable.core

import java.net.InetSocketAddress

/**
 * A node in a HTable cluster that serves nodes.
 */
final case class Node(uid: String, address: InetSocketAddress)
