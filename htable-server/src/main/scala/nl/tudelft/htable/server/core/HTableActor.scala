package nl.tudelft.htable.server.core

import akka.Done
import akka.actor.typed._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.TypedActorSystemOps
import nl.tudelft.htable.client.{CachingServiceResolver, DefaultServiceResolverImpl, HTableClient, HTableInternalClient}
import nl.tudelft.htable.core._
import nl.tudelft.htable.server.core.services.{AdminServiceImpl, ClientServiceImpl, InternalServiceImpl}
import nl.tudelft.htable.server.core.util.ServerServiceResolver
import nl.tudelft.htable.storage.StorageDriver
import org.apache.curator.framework.CuratorFramework

import scala.collection.mutable
import scala.concurrent.Promise

object HTableActor {

  /**
   * Internal commands that are accepted by the [HTableServer].
   */
  sealed trait Command

  /**
   * Request the server to create a new table.
   */
  final case class CreateTable(name: String, promise: Promise[Done]) extends Command

  /**
   * Request the server to delete a table.
   */
  final case class DeleteTable(name: String, promise: Promise[Done]) extends Command

  /**
   * Internal message wrapper for ZooKeeper event.
   */
  private final case class ZooKeeperEvent(event: ZooKeeperActor.Event) extends Command

  /**
   * Construct the main logic of the server.
   *
   * @param self The node to represent.
   * @param zk The ZooKeeper client.
   * @param storageDriver The storage driver to use.
   */
  def apply(self: Node, zk: CuratorFramework, storageDriver: StorageDriver): Behavior[Command] =
    Behaviors
      .setup[Command] { context =>
        implicit val sys: ActorSystem[Nothing] = context.system

        context.log.info("Booting HTable server")

        val nodeActor = context.spawn(NodeActor(self, storageDriver), name = "node")
        context.watch(nodeActor)

        val clientService = new ClientServiceImpl(nodeActor)
        val adminService = new AdminServiceImpl(context.self)
        val internalService = new InternalServiceImpl(nodeActor)

        val client = HTableClient.createInternal(
          zk,
          context.system.toClassic,
          new ServerServiceResolver(
            self,
            new CachingServiceResolver(new DefaultServiceResolverImpl(context.system.toClassic)),
            clientService,
            adminService,
            internalService)
        )

        val grpc =
          context.spawn(GRPCActor(self.address, clientService, adminService, internalService), name = "grpc-server")
        context.watch(grpc)

        val adapter = context.messageAdapter(HTableActor.ZooKeeperEvent)
        val zkRef = context.spawn(ZooKeeperActor(self, zk, adapter), name = "zookeeper")
        context.watch(zkRef)
        started(self, zkRef, client, storageDriver)
      }

  /**
   * Construct the behavior of the server when it has started.
   *
   * @param self The self that has been spawned.
   * @param zk The reference to the ZooKeeper actor.
   * @param client The client to communicate with other nodes.
   * @param storageDriver The storage driver to use.
   */
  def started(self: Node,
              zk: ActorRef[ZooKeeperActor.Command],
              client: HTableInternalClient,
              storageDriver: StorageDriver): Behavior[Command] =
    Behaviors.setup { context =>
      val nodes = new mutable.HashSet[Node]()
      Behaviors
        .receiveMessage[Command] {
          case ZooKeeperEvent(ZooKeeperActor.Elected) =>
            context.log.info("Node has been elected")
            master(self, nodes, zk, client, storageDriver)
          case ZooKeeperEvent(ZooKeeperActor.NodeJoined(node)) =>
            context.log.info(s"Node ${node.uid} has joined")
            nodes += node
            Behaviors.same
          case ZooKeeperEvent(ZooKeeperActor.NodeLeft(node)) =>
            context.log.info(s"Node ${node.uid} has left")
            nodes -= node
            Behaviors.same
          case CreateTable(_, promise) =>
            promise.failure(new NotImplementedError())
            Behaviors.same
          case DeleteTable(_, promise) =>
            promise.failure(new NotImplementedError())
            Behaviors.same
          case _ => throw new IllegalArgumentException()
        }
    }

  /**
   * Construct the behavior of the server when it becomes the master.
   *
   * @param self The self that has been spawned.
   * @param nodes The active nodes in the cluster.
   * @param zk The reference to the ZooKeeper actor.
   * @param client The client to communicate with other nodes.
   * @param storageDriver The storage driver to use.
   */
  def master(self: Node,
             nodes: mutable.Set[Node],
             zk: ActorRef[ZooKeeperActor.Command],
             client: HTableInternalClient,
             storageDriver: StorageDriver): Behavior[Command] =
    Behaviors.setup { context =>
      // Spawn the load balancer
      val loadBalancer = context.spawn(LoadBalancerActor(zk, client), name = "load-balancer")
      context.watch(loadBalancer)
      loadBalancer ! LoadBalancerActor.Start(nodes.toSet)

      Behaviors
        .receiveMessage[Command] {
          case ZooKeeperEvent(ZooKeeperActor.Overthrown) =>
            context.log.info("Node has been overthrown")
            Behaviors.stopped
          case ZooKeeperEvent(ZooKeeperActor.NodeJoined(node)) =>
            context.log.info(s"Node ${node.uid} has joined")
            nodes += node

            // Start a load balancing cycle
            loadBalancer ! LoadBalancerActor.Start(nodes.toSet)

            Behaviors.same
          case ZooKeeperEvent(ZooKeeperActor.NodeLeft(node)) =>
            context.log.info(s"Node ${node.uid} has left")
            nodes -= node

            // Start a load balancing cycle
            loadBalancer ! LoadBalancerActor.Start(nodes.toSet)

            Behaviors.same
          case CreateTable(_, promise) =>
            promise.failure(new NotImplementedError())
            Behaviors.same
          case DeleteTable(_, promise) =>
            promise.failure(new NotImplementedError())
            Behaviors.same
          case _ => throw new IllegalArgumentException()
        }

    }
}
