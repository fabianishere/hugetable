package nl.tudelft.htable.server.core

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

object HTableActor {

  /**
   * Internal commands that are accepted by the [HTableServer].
   */
  sealed trait Command

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

        // Spawn the node actor
        val nodeActor = context.spawn(NodeActor(self, storageDriver), name = "node")
        // Kill ourselves if the child dies
        context.watch(nodeActor)

        // Spawn the admin actor
        val admin = context.spawn(AdminActor(), name = "admin")
        context.watch(admin)

        // Create the client for communication with other nodes
        val clientService = new ClientServiceImpl(nodeActor)
        val adminService = new AdminServiceImpl(admin)
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

        // Spawn the gRPC services actor
        val grpc =
          context.spawn(GRPCActor(self.address, clientService, adminService, internalService), name = "grpc-server")
        context.watch(grpc)

        // Spawn the ZooKeeper actor
        val adapter = context.messageAdapter(HTableActor.ZooKeeperEvent)
        val zkRef = context.spawn(ZooKeeperActor(self, zk, adapter), name = "zookeeper")
        context.watch(zkRef)

        // Spawn the load balancer
        val loadBalancer = context.spawn(LoadBalancerActor(zkRef, client), name = "load-balancer")
        context.watch(loadBalancer)

        started(self, client, admin, loadBalancer)
      }

  /**
   * Construct the behavior of the server when it has started.
   *
   * @param self The self that has been spawned.
   * @param client The client to communicate with other nodes.
   * @param admin The admin actor.
   * @param isMaster A flag to indicate the node is a master.
   * @param nodes The active nodes in the cluster.
   */
  def started(self: Node,
              client: HTableInternalClient,
              admin: ActorRef[AdminActor.Command],
              loadBalancer: ActorRef[LoadBalancerActor.Command],
              isMaster: Boolean = false,
              nodes: mutable.Set[Node] = mutable.Set.empty): Behavior[Command] =
    Behaviors.setup { context =>
      val nodes = new mutable.HashSet[Node]()
      Behaviors
        .receiveMessage[Command] {
          case ZooKeeperEvent(ZooKeeperActor.Elected) =>
            context.log.info("Node has been elected")
            // Enable admin endpoint
            admin ! AdminActor.Enable(client)

            // Schedule a load balancing job
            loadBalancer ! LoadBalancerActor.Schedule(nodes.toSet)

            started(self, client, admin, loadBalancer, isMaster = true, nodes)
          case ZooKeeperEvent(ZooKeeperActor.Overthrown) =>
            context.log.info("Node has been overthrown")
            Behaviors.stopped
          case ZooKeeperEvent(ZooKeeperActor.NodeJoined(node)) =>
            context.log.info(s"Node ${node.uid} has joined")
            nodes += node

            if (isMaster) {
              // Start a load balancing cycle
              loadBalancer ! LoadBalancerActor.Schedule(nodes.toSet)
            }

            Behaviors.same
          case ZooKeeperEvent(ZooKeeperActor.NodeLeft(node)) =>
            context.log.info(s"Node ${node.uid} has left")
            nodes -= node

            if (isMaster) {
              // Start a load balancing cycle
              loadBalancer ! LoadBalancerActor.Schedule(nodes.toSet)
            }

            Behaviors.same
          case _ => throw new IllegalArgumentException()
        }
    }
}
