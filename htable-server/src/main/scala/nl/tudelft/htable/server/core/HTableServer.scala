package nl.tudelft.htable.server.core

import java.util

import akka.NotUsed
import akka.actor.typed.scaladsl.adapter.TypedActorSystemOps
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed._
import akka.grpc.scaladsl.ServiceHandler
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.{Http, HttpConnectionContext}
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import nl.tudelft.htable.core.{Node, Tablet}
import nl.tudelft.htable.protocol.admin._
import nl.tudelft.htable.protocol.client._
import nl.tudelft.htable.protocol.internal._
import org.apache.curator.framework.CuratorFramework

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object HTableServer {

  /**
   * Internal commands that are accepted by the [HTableServer].
   */
  private sealed trait Command

  /**
   * Internal message indicating that the gRPC service is up.
   */
  private final case class ServiceUp(binding: Http.ServerBinding) extends Command

  /**
   * Internal message indicating that the gRPC service is down.
   */
  private final case class ServiceDown(throwable: Throwable) extends Command

  /**
   * Internal message wrapper for ZooKeeper event.
   */
  private final case class ZooKeeperEvent(event: ZooKeeperManager.Event) extends Command

  /**
   * Construct the main logic of the server.
   *
   * @param uid The unique identifier of the server.
   * @param zk The ZooKeeper client.
   */
  def apply(uid: String, zk: CuratorFramework): Behavior[NodeManager.Command] =
    Behaviors
      .setup[AnyRef] { context =>
        context.log.info("Booting HTable server")

        context.log.info("Starting gRPC services")
        context.pipeToSelf(createServices(context)) {
          case Success(value) => ServiceUp(value)
          case Failure(e)     => ServiceDown(e)
        }

        Behaviors
          .receiveMessagePartial[AnyRef] {
            case ServiceUp(binding) =>
              context.log.info(s"Listening to ${binding.localAddress}")

              val node = Node(uid, binding.localAddress)
              val adapter = context.messageAdapter(HTableServer.ZooKeeperEvent)
              val zkRef = context.spawn(ZooKeeperManager(zk, node, adapter), name = "zookeeper")
              context.watch(zkRef)

              started(node, binding, zkRef)
            case ServiceDown(e) => throw e
          }
      }
      .narrow

  /**
   * Construct the behavior of the server when it has started.
   *
   * @param self The self that has been spawned.
   * @param binding The server binding for the gRPC services.
   * @param zkRef The reference to the ZooKeeper actor.
   */
  def started(self: Node, binding: Http.ServerBinding, zkRef: ActorRef[ZooKeeperManager.Command]): Behavior[AnyRef] =
    Behaviors.setup { context =>
      val nodes = new mutable.HashSet[Node]()
      val tablets = new util.TreeMap[Tablet, ActorRef[TabletManager.Command]]()

      Behaviors
        .receiveMessage[AnyRef] {
          case ZooKeeperEvent(ZooKeeperManager.Elected) =>
            context.log.info("Node has been elected")
            master(self, nodes.toSet, binding, zkRef)
          case ZooKeeperEvent(ZooKeeperManager.NodeJoined(node)) =>
            context.log.info(s"Node ${node.uid} has joined")
            nodes += node
            Behaviors.same
          case ZooKeeperEvent(ZooKeeperManager.NodeLeft(node)) =>
            context.log.info(s"Node ${node.uid} has left")
            nodes -= node
            Behaviors.same
          case NodeManager.Ping(replyTo) =>
            replyTo ! NodeManager.Pong(self)
            Behaviors.same
          case _ => throw new IllegalArgumentException()
        }
        .receiveSignal {
          case (_, Terminated(ref)) if ref == zkRef =>
            throw new IllegalStateException("ZooKeeper actor has terminated")
          case (_, PostStop) =>
            context.log.info("HugeTable server stopping")
            binding.terminate(10.seconds)
            Behaviors.same
        }
    }

  /**
   * Construct the behavior of the server when it becomes the master.
   *
   * @param self The self that has been spawned.
   * @param nodes The active nodes in the cluster.
   * @param binding The server binding for the gRPC services.
   * @param zkRef The reference to the ZooKeeper actor.
   */
  def master(self: Node,
             nodes: Set[Node],
             binding: Http.ServerBinding,
             zkRef: ActorRef[ZooKeeperManager.Command]): Behavior[AnyRef] =
    Behaviors.setup { context =>
      // Spawn actors for the active nodes
      val nodeRefs = mutable.HashMap[Node, ActorRef[NodeManager.Command]]((self, context.self))
      for (node <- nodes if node != self) {
        nodeRefs(node) = context.spawn(NodeManager(node), name = s"node-${node.uid}")
      }

      // Spawn the load balancer
      val loadBalancer = context.spawn(LoadBalancer(nodeRefs.toMap), name = "load-balancer")

      Behaviors
        .receiveMessage[AnyRef] {
          case ZooKeeperEvent(ZooKeeperManager.Overthrown) =>
            context.log.info("Node has been overthrown")
            Behaviors.stopped
          case ZooKeeperEvent(ZooKeeperManager.NodeJoined(node)) =>
            context.log.info(s"Node ${node.uid} has joined")
            nodeRefs(node) = context.spawn(NodeManager(node), name = s"node-${node.uid}")
            Behaviors.same
          case ZooKeeperEvent(ZooKeeperManager.NodeLeft(node)) =>
            context.log.info(s"Node ${node.uid} has left")

            // Kill node manager and remove from map
            nodeRefs.remove(node) match {
              case Some(nodeRef) => context.stop(nodeRef)
              case _ =>
            }

            Behaviors.same
          case NodeManager.Ping(replyTo) =>
            replyTo ! NodeManager.Pong(self)
            Behaviors.same
          case _ => throw new IllegalArgumentException()
        }
        .receiveSignal {
          case (_, Terminated(ref)) if ref == zkRef =>
            throw new IllegalStateException("ZooKeeper actor has terminated")
          case (_, PostStop) =>
            context.log.info("HugeTable server stopping")
            binding.terminate(10.seconds)
            Behaviors.same
        }
    }

  /**
   * Create the gRPC services.
   */
  private def createServices(context: ActorContext[_]): Future[Http.ServerBinding] = {
    // Akka boot up code
    implicit val sys: ActorSystem[Nothing] = context.system
    implicit val classicSys: akka.actor.ActorSystem = context.system.toClassic
    implicit val mat: Materializer = Materializer(context.system)
    implicit val ec: ExecutionContext =
      context.system.dispatchers.lookup(DispatcherSelector.default())

    val client = ClientServiceHandler.partial(new ClientServiceImpl)
    val admin = AdminServiceHandler.partial(new AdminServiceImpl)
    val internal = InternalServiceHandler.partial(new InternalServiceImpl)

    // Create service handlers
    val service: HttpRequest => Future[HttpResponse] =
      ServiceHandler.concatOrNotFound(client, admin, internal)

    // Bind service handler servers to localhost
    Http().bindAndHandleAsync(
      service,
      interface = "127.0.0.1",
      port = 0, // Let the OS assign some port to us.
      connectionContext = HttpConnectionContext()
    )
  }

  private class ClientServiceImpl(implicit val mat: Materializer) extends ClientService {

    /**
     * Read the specified row (range) and stream back the response.
     */
    override def read(in: ReadRequest): Source[ReadResponse, NotUsed] = {
      Source.single(ReadResponse())
    }

    /**
     * Mutate a specified row in a table.
     */
    override def mutate(in: MutateRequest): Future[MutateResponse] = {
      Future.successful(MutateResponse())
    }
  }

  private class AdminServiceImpl(implicit val mat: Materializer) extends AdminService {

    /**
     * Create a new table in the cluster.
     */
    override def createTable(in: CreateTableRequest): Future[CreateTableResponse] = ???

    /**
     * Delete a table in the cluster.
     */
    override def deleteTable(in: DeleteTableRequest): Future[DeleteTableResponse] = ???
  }

  private class InternalServiceImpl(implicit mat: Materializer) extends InternalService {

    /**
     * Ping a self in the cluster.
     */
    override def ping(in: PingRequest): Future[PingResponse] = ???

    /**
     * Query a self for the tablets it's serving.
     */
    override def query(in: QueryRequest): Future[QueryResponse] = ???

    /**
     * Assign the specified tablets to the self.
     */
    override def assign(in: AssignRequest): Future[AssignResponse] = ???
  }
}
