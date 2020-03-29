package nl.tudelft.htable.server.core

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, DispatcherSelector, PostStop}
import nl.tudelft.htable.core.Node
import nl.tudelft.htable.protocol.ClientAdapters._
import nl.tudelft.htable.protocol.CoreAdapters
import nl.tudelft.htable.server.core.curator.{GroupMember, GroupMemberListener}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{ChildData, NodeCache}
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}
import org.apache.curator.framework.recipes.nodes.PersistentNode
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.utils.ZKPaths
import org.apache.zookeeper.CreateMode

import scala.language.implicitConversions

/**
 * An actor for managing the ZooKeeper connection.
 */
object ZooKeeperManager {

  /**
   * Commands that are accepted by the [ZooKeeperManager].
   */
  sealed trait Command

  /**
   * Internal message indicating ZooKeeper connection was successful.
   */
  private final case object Connected extends Command

  /**
   * Internal message indicating ZooKeeper disconnected.
   */
  private final case object Disconnected extends Command

  /**
   * Message to claim the root tablet.
   */
  final case object ClaimRoot extends Command

  /**
   * Message to unclaim the root tablet.
   */
  final case object UnclaimRoot extends Command

  /**
   * Events emitted by the [ZooKeeperManager].
   */
  sealed trait Event

  /**
   * Internal message indicating that the server was elected to be the leader.
   */
  final case object Elected extends Event

  /**
   * Internal message indicating that the server was overthrown.
   */
  final case object Overthrown extends Event

  /**
   * Internal message sent when a new self has joined the cluster.
   */
  final case class NodeJoined(node: Node) extends Event

  /**
   * Internal message sent when a self has left the cluster.
   */
  final case class NodeLeft(node: Node) extends Event

  /**
   * Event emitted when the root tablet location was updated.
   */
  final case class RootUpdated(node: Option[Node]) extends Event

  /**
   * Construct the behavior for the ZooKeeper manager.
   *
   * @param zookeeper The ZooKeeper client to use.
   * @param node The self to open the connection for.
   * @param listener The listener to emit events to.
   */
  def apply(zookeeper: CuratorFramework, node: Node, listener: ActorRef[Event]): Behavior[Command] =
    Behaviors.setup { context =>
      context.log.info("Connecting to ZooKeeper")

      zookeeper.start()
      zookeeper.getConnectionStateListenable.addListener((_: CuratorFramework, newState: ConnectionState) =>
        if (newState.isConnected) {
          context.self ! Connected
        } else {
          context.self ! Disconnected
      })

      Behaviors
        .receiveMessage[Command] {
          case Connected    => connected(zookeeper, node, listener)
          case Disconnected => Behaviors.stopped
          case _            => throw new IllegalStateException()
        }
        .receiveSignal {
          case (_, PostStop) =>
            zookeeper.close()
            Behaviors.same
        }
    }

  /**
   * Construct the behavior for when the ZooKeeper client is connected.
   */
  private def connected(zookeeper: CuratorFramework, node: Node, listener: ActorRef[Event]): Behavior[Command] =
    Behaviors.setup { context =>
      context.log.info("Joining leader election")

      // Create group membership
      val membership =
        new GroupMember(zookeeper, "/servers", node.uid, CoreAdapters.serializeAddress(node.address))
      membership.addListener(new GroupMemberListener {
        override def memberJoined(data: ChildData): Unit = listener ! NodeJoined(data)
        override def memberLeft(data: ChildData): Unit = listener ! NodeLeft(data)
      })
      membership.start()

      // Perform leader election via ZooKeeper
      val leaderLatch = new LeaderLatch(zookeeper, "/leader", node.uid)
      leaderLatch.addListener(
        new LeaderLatchListener {
          override def isLeader(): Unit = listener ! Elected
          override def notLeader(): Unit = listener ! Overthrown
        },
        context.system.dispatchers.lookup(DispatcherSelector.blocking())
      )
      leaderLatch.start()

      // Claim root
      var rootClaim: Option[PersistentNode] = None

      // Watch root
      val cache = new NodeCache(zookeeper, "/root")
      cache.getListenable.addListener(() => listener ! RootUpdated(Option(cache.getCurrentData).map(toNode)))
      cache.start()

      Behaviors
        .receiveMessage[Command] {
          case ClaimRoot =>
            context.log.info("Claiming root")
            val pen = new PersistentNode(zookeeper,
                                         CreateMode.EPHEMERAL,
                                         false,
                                         "/root",
                                         CoreAdapters.serializeAddress(node.address))
            pen.start()
            rootClaim = Some(pen)
            Behaviors.same
          case UnclaimRoot =>
            context.log.info("Unclaiming root")
            rootClaim.foreach(_.close())
            rootClaim = None
            Behaviors.same
          case Disconnected => throw new IllegalStateException("ZooKeeper has disconnected")
          case _            => throw new IllegalStateException()
        }
        .receiveSignal {
          case (_, PostStop) =>
            cache.close()
            leaderLatch.close()
            membership.close()
            zookeeper.close()
            Behaviors.same
        }
    }

  /**
   * Convert [ChildData] into [Node].
   */
  private implicit def toNode(data: ChildData): Node =
    Node(ZKPaths.getNodeFromPath(data.getPath), CoreAdapters.deserializeAddress(data.getData))
}
