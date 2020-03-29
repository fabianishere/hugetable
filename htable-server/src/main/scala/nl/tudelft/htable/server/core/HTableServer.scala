package nl.tudelft.htable.server.core

import java.util

import akka.NotUsed
import akka.actor.typed._
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter.TypedActorSystemOps
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.{Http, HttpConnectionContext}
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.Timeout
import nl.tudelft.htable.core.{Get, Node, Row, RowRange, Scan, Tablet}
import nl.tudelft.htable.protocol.SerializationUtils
import nl.tudelft.htable.protocol.SerializationUtils._
import nl.tudelft.htable.protocol.admin.{
  AdminService,
  AdminServiceHandler,
  CreateTableRequest,
  CreateTableResponse,
  DeleteTableRequest,
  DeleteTableResponse
}
import nl.tudelft.htable.protocol.client.{
  ClientService,
  ClientServiceHandler,
  MutateRequest,
  MutateResponse,
  ReadRequest,
  ReadResponse
}
import nl.tudelft.htable.protocol.internal.{
  AssignRequest,
  AssignResponse,
  InternalService,
  InternalServiceHandler,
  PingRequest,
  PingResponse,
  QueryRequest,
  QueryResponse
}
import nl.tudelft.htable.server.core.util.AkkaServiceHandler
import nl.tudelft.htable.storage.StorageDriver
import org.apache.curator.framework.CuratorFramework

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
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
   * @param storageDriver The storage driver to use.
   */
  def apply(uid: String, zk: CuratorFramework, storageDriver: StorageDriver): Behavior[NodeManager.Command] =
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

              started(node, binding, zkRef, storageDriver)
            case ServiceDown(e) => throw e
          }
      }
      .narrow

  /**
   * Construct the behavior of the server when it has started.
   *
   * @param self The self that has been spawned.
   * @param binding The server binding for the gRPC services.
   * @param zkRef   The reference to the ZooKeeper actor.
   * @param storageDriver The storage driver to use.
   */
  def started(self: Node,
              binding: Http.ServerBinding,
              zkRef: ActorRef[ZooKeeperManager.Command],
              storageDriver: StorageDriver): Behavior[AnyRef] =
    Behaviors.setup { context =>
      val nodes = new mutable.HashSet[Node]()
      val tablets = new util.TreeMap[Tablet, ActorRef[NodeManager.Command]]()
      var root: Option[Node] = None

      Behaviors
        .receiveMessage[AnyRef] {
          case ZooKeeperEvent(ZooKeeperManager.Elected) =>
            context.log.info("Node has been elected")
            master(self, nodes.toSet, root, tablets, binding, zkRef, storageDriver)
          case ZooKeeperEvent(ZooKeeperManager.NodeJoined(node)) =>
            context.log.info(s"Node ${node.uid} has joined")
            nodes += node
            Behaviors.same
          case ZooKeeperEvent(ZooKeeperManager.NodeLeft(node)) =>
            context.log.info(s"Node ${node.uid} has left")
            nodes -= node
            Behaviors.same
          case ZooKeeperEvent(ZooKeeperManager.RootUpdated(node)) =>
            context.log.info(s"Root updated to $node")
            root = node
            Behaviors.same
          case NodeManager.Ping(replyTo) =>
            replyTo ! NodeManager.Pong(self)
            Behaviors.same
          case NodeManager.QueryTablets(replyTo) =>
            replyTo ! NodeManager.QueryTabletsResponse(self, tablets.keySet().asScala.toSeq)
            Behaviors.same
          case NodeManager.Assign(newTablets) =>
            tablets.forEach {
              case (tablet, ref) =>
                if (Tablet.isRoot(tablet)) {
                  zkRef ! ZooKeeperManager.UnclaimRoot
                }
                context.stop(ref)
            }
            tablets.clear()

            // Spawn new tablet managers
            for (tablet <- newTablets) {
              if (Tablet.isRoot(tablet)) {
                zkRef ! ZooKeeperManager.ClaimRoot
              }
              tablets.put(tablet, context.spawnAnonymous(TabletManager(storageDriver, tablet)))
            }

            Behaviors.same
          case NodeManager.Read(query, replyTo) =>
            query match {
              case Get(table, key) =>
                Option(tablets.floorEntry(Tablet(table, RowRange.leftBounded(key)))) match {
                  case Some(entry) => entry.getValue ! NodeManager.Read(query, replyTo)
                  case None        =>
                }
              case Scan(table, range) =>
                implicit val timeout: Timeout = 3.seconds
                implicit val sys: ActorSystem[Nothing] = context.system

                val start = Tablet(table, range)
                val end = Tablet(table, RowRange.leftBounded(range.end))

                val source: Source[Row, NotUsed] =
                  Source(tablets.subMap(start, true, end, true).entrySet().asScala.toSeq)
                    .map(entry => entry.getValue.ask[NodeManager.ReadResponse](NodeManager.Read(Scan(table, range), _)))
                    .flatMapConcat[NodeManager.ReadResponse, NotUsed](Source.future)
                    .flatMapConcat(_.rows)

                replyTo ! NodeManager.ReadResponse(source)
            }
            Behaviors.same
          case NodeManager.Mutate(mutation, replyTo) =>
            Option(tablets.floorEntry(Tablet(mutation.table, RowRange.leftBounded(mutation.key)))) match {
              case Some(entry) => entry.getValue ! NodeManager.Mutate(mutation, replyTo)
              case None        =>
            }
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
   * @param tablets The tablets assigned to this server.
   * @param binding The server binding for the gRPC services.
   * @param zkRef The reference to the ZooKeeper actor.
   * @param storageDriver The storage driver to use.
   */
  def master(self: Node,
             nodes: Set[Node],
             oldRoot: Option[Node],
             tablets: util.TreeMap[Tablet, ActorRef[NodeManager.Command]],
             binding: Http.ServerBinding,
             zkRef: ActorRef[ZooKeeperManager.Command],
             storageDriver: StorageDriver): Behavior[AnyRef] =
    Behaviors.setup { context =>
      var root = oldRoot

      // Spawn actors for the active nodes
      val nodeRefs = mutable.HashMap[Node, ActorRef[NodeManager.Command]]((self, context.self))
      for (node <- nodes if node != self) {
        nodeRefs(node) = context.spawn(NodeManager(node), name = s"node-${node.uid}")
      }

      // Spawn the load balancer
      val loadBalancer = context.spawn(LoadBalancer(), name = "load-balancer")
      loadBalancer ! LoadBalancer.Start(nodeRefs.toMap)

      Behaviors
        .receiveMessage[AnyRef] {
          case ZooKeeperEvent(ZooKeeperManager.Overthrown) =>
            context.log.info("Node has been overthrown")
            Behaviors.stopped
          case ZooKeeperEvent(ZooKeeperManager.NodeJoined(node)) =>
            context.log.info(s"Node ${node.uid} has joined")
            nodeRefs(node) = context.spawn(NodeManager(node), name = s"node-${node.uid}")

            // Start a load balancing cycle
            loadBalancer ! LoadBalancer.Start(nodeRefs.toMap)

            Behaviors.same
          case ZooKeeperEvent(ZooKeeperManager.NodeLeft(node)) =>
            context.log.info(s"Node ${node.uid} has left")

            // Kill node manager and remove from map
            nodeRefs.remove(node) match {
              case Some(nodeRef) => context.stop(nodeRef)
              case _             =>
            }

            // Start a load balancing cycle
            loadBalancer ! LoadBalancer.Start(nodeRefs.toMap)

            Behaviors.same
          case ZooKeeperEvent(ZooKeeperManager.RootUpdated(node)) =>
            context.log.info(s"Root updated to $node")
            root = node
            Behaviors.same
          case NodeManager.Ping(replyTo) =>
            replyTo ! NodeManager.Pong(self)
            Behaviors.same
          case NodeManager.QueryTablets(replyTo) =>
            replyTo ! NodeManager.QueryTabletsResponse(self, tablets.keySet().asScala.toSeq)
            Behaviors.same
          case NodeManager.Assign(newTablets) =>
            tablets.forEach {
              case (tablet, ref) =>
                if (Tablet.isRoot(tablet)) {
                  zkRef ! ZooKeeperManager.UnclaimRoot
                }
                context.stop(ref)
            }
            tablets.clear()

            // Spawn new tablet managers
            for (tablet <- newTablets) {
              if (Tablet.isRoot(tablet)) {
                zkRef ! ZooKeeperManager.ClaimRoot
              }
              tablets.put(tablet, context.spawnAnonymous(TabletManager(storageDriver, tablet)))
            }

            Behaviors.same
          case NodeManager.Read(query, replyTo) =>
            query match {
              case Get(table, key) =>
                Option(tablets.floorEntry(Tablet(table, RowRange.leftBounded(key)))) match {
                  case Some(entry) => entry.getValue ! NodeManager.Read(query, replyTo)
                  case None        =>
                }
              case Scan(table, range) =>
                implicit val timeout: Timeout = 3.seconds
                implicit val sys: ActorSystem[Nothing] = context.system

                val start = Tablet(table, range)
                val end = Tablet(table, RowRange.leftBounded(range.end))

                val source: Source[Row, NotUsed] =
                  Source(tablets.subMap(start, true, end, true).entrySet().asScala.toSeq)
                    .map(entry => entry.getValue.ask[NodeManager.ReadResponse](NodeManager.Read(Scan(table, range), _)))
                    .flatMapConcat[NodeManager.ReadResponse, NotUsed](Source.future)
                    .flatMapConcat(_.rows)

                replyTo ! NodeManager.ReadResponse(source)
            }
            Behaviors.same
          case NodeManager.Mutate(mutation, replyTo) =>
            Option(tablets.floorEntry(Tablet(mutation.table, RowRange.leftBounded(mutation.key)))) match {
              case Some(entry) => entry.getValue ! NodeManager.Mutate(mutation, replyTo)
              case None        =>
            }
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
  private def createServices(context: ActorContext[AnyRef]): Future[Http.ServerBinding] = {
    // Akka boot up code
    implicit val sys: ActorSystem[Nothing] = context.system
    implicit val classicSys: akka.actor.ActorSystem = context.system.toClassic
    implicit val mat: Materializer = Materializer(context.system)
    implicit val ec: ExecutionContext =
      context.system.dispatchers.lookup(DispatcherSelector.default())

    val client = ClientServiceHandler.partial(new ClientServiceImpl(context.self))
    val admin = AdminServiceHandler.partial(new AdminServiceImpl(context.self))
    val internal = InternalServiceHandler.partial(new InternalServiceImpl(context.self))

    // Create service handlers
    val service: HttpRequest => Future[HttpResponse] =
      AkkaServiceHandler.concatOrNotFound(client, admin, internal)

    // Bind service handler servers to localhost
    Http().bindAndHandleAsync(
      service,
      interface = "127.0.0.1",
      port = 0, // Let the OS assign some port to us.
      connectionContext = HttpConnectionContext()
    )
  }

  private class ClientServiceImpl(self: ActorRef[NodeManager.Command])(implicit val sys: ActorSystem[Nothing])
      extends ClientService {
    implicit val timeout: Timeout = 3.seconds
    implicit val ec: ExecutionContext = sys.dispatchers.lookup(DispatcherSelector.default())

    /**
     * Read the specified row (range) and stream back the response.
     */
    override def read(in: ReadRequest): Source[ReadResponse, NotUsed] = {
      Source
        .future(self.ask[NodeManager.ReadResponse](ref => NodeManager.Read(toQuery(in), ref)))
        .flatMapConcat(_.rows)
        .map(row => ReadResponse(cells = row.cells.map(cell => SerializationUtils.toPBCell(row, cell))))
    }

    /**
     * Mutate a specified row in a table.
     */
    override def mutate(in: MutateRequest): Future[MutateResponse] = {
      self
        .ask[NodeManager.MutateResponse.type](NodeManager.Mutate(SerializationUtils.toRowMutation(in), _))
        .map(_ => MutateResponse())
    }
  }

  private class AdminServiceImpl(self: ActorRef[NodeManager.Command])(implicit val sys: ActorSystem[Nothing])
      extends AdminService {

    /**
     * Create a new table in the cluster.
     */
    override def createTable(in: CreateTableRequest): Future[CreateTableResponse] = ???

    /**
     * Delete a table in the cluster.
     */
    override def deleteTable(in: DeleteTableRequest): Future[DeleteTableResponse] = ???
  }

  private class InternalServiceImpl(self: ActorRef[NodeManager.Command])(implicit val sys: ActorSystem[Nothing])
      extends InternalService {
    // asking someone requires a timeout if the timeout hits without response
    // the ask is failed with a TimeoutException
    implicit val timeout: Timeout = 3.second
    implicit val ec: ExecutionContext = sys.dispatchers.lookup(DispatcherSelector.default())

    /**
     * Ping a self in the cluster.
     */
    override def ping(in: PingRequest): Future[PingResponse] = self.ask(NodeManager.Ping).map(_ => PingResponse())

    /**
     * QueryTablets a self for the tablets it's serving.
     */
    override def query(in: QueryRequest): Future[QueryResponse] =
      self
        .ask(NodeManager.QueryTablets)
        .map(res => QueryResponse(res.tablets.map(t => t)))

    /**
     * Assign the specified tablets to the self.
     */
    override def assign(in: AssignRequest): Future[AssignResponse] = {
      self ! NodeManager.Assign(in.tablets.map(t => t))
      Future.successful(AssignResponse())
    }
  }
}
