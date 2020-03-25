package nl.tudelft.htable.server.core

import java.util.UUID

import akka.NotUsed
import akka.actor.typed.scaladsl.adapter.TypedActorSystemOps
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{Behavior, DispatcherSelector, PostStop}
import akka.grpc.scaladsl.ServiceHandler
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.{Http, HttpConnectionContext}
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import nl.tudelft.htable.protocol.admin._
import nl.tudelft.htable.protocol.client._
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}
import org.apache.curator.framework.recipes.nodes.GroupMember

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object HTableServer {

  /**
   * Commands that are accepted by the [HTableServer].
   */
  sealed trait Command

  /**
   * Internal message indicating that the gRPC service is up.
   */
  private final case class ServiceUp(binding: Http.ServerBinding) extends Command

  /**
   * Internal message indicating that the gRPC service is down.
   */
  private final case class ServiceDown(throwable: Throwable) extends Command

  /**
   * Internal message indicating that the server was elected to be the leader.
   */
  private final case object Elected extends Command

  /**
   * Internal message indicating that the server was overthrown.
   */
  private final case object Overthrown extends Command

  /**
   * Construct the main logic of the server.
   */
  def apply(zookeeper: CuratorFramework): Behavior[Command] =
    Behaviors.setup(context => {
      val server = new HTableServer(context, zookeeper)
      server.init()
    })
}

/**
 * Main implementation of a tablet server as described in the Google BigTable paper.
 *
 * @param context The actor context to run in.
 * @param zookeeper The client to communicate with the ZooKeeper cluster.
 */
class HTableServer(private val context: ActorContext[HTableServer.Command], private val zookeeper: CuratorFramework) {
  // Akka boot up code
  implicit val sys: akka.actor.ActorSystem = context.system.toClassic
  implicit val mat: Materializer = Materializer(context.system)
  implicit val ec: ExecutionContext =
    context.system.dispatchers.lookup(DispatcherSelector.default())

  /**
   * The unique identifier of the server.
   */
  private val uid = UUID.randomUUID().toString

  context.log.info("Booting HTable server")
  context.log.info("Starting gRPC services")
  context.pipeToSelf(createServices()) {
    case Success(value) => HTableServer.ServiceUp(value)
    case Failure(e)     => HTableServer.ServiceDown(e)
  }

  /**
   * Construct the initial behavior of the server.
   */
  def init(): Behavior[HTableServer.Command] =
    Behaviors
      .receiveMessagePartial[HTableServer.Command] {
        case HTableServer.ServiceUp(binding) =>
          context.log.info(s"Listening to ${binding.localAddress}")
          start(binding)
        case HTableServer.ServiceDown(e) =>
          context.log.error("Failed to start gRPC services", e)
          Behaviors.same
      }

  /**
   * Construct the behavior to start the server.
   *
   * @param binding The server binding for the gRPC services.
   */
  def start(binding: Http.ServerBinding): Behavior[HTableServer.Command] =
    Behaviors.setup { _ =>
      context.log.info("Joining ZooKeeper group")

      val leaderLatch = new LeaderLatch(zookeeper, "/master", uid)
      val membership =
        new GroupMember(zookeeper, "/servers", uid, Serialization.serialize(binding.localAddress))

      leaderLatch.addListener(new LeaderLatchListener {
        override def isLeader(): Unit = context.self.tell(HTableServer.Elected)
        override def notLeader(): Unit = context.self.tell(HTableServer.Overthrown)
      }, context.system.dispatchers.lookup(DispatcherSelector.blocking()))
      leaderLatch.start()
      membership.start()

      started(binding, leaderLatch, membership)
    }

  /**
   * Construct the behavior of the server when it has started.
   *
   * @param binding The server binding for the gRPC services.
   * @param leaderLatch The latch for determining the master server.
   * @param membership The [GroupMember] instance for keeping track of the tablet servers via ZooKeeper.
   */
  def started(binding: Http.ServerBinding,
              leaderLatch: LeaderLatch,
              membership: GroupMember): Behavior[HTableServer.Command] =
    Behaviors
      .receiveMessage[HTableServer.Command] {
        case HTableServer.Elected =>
          context.log.info("Node is elected for leader")
          master(binding, leaderLatch, membership)
      }
      .receiveSignal {
        case (_, PostStop) =>
          context.log.info("HugeTable server stopping")
          binding.terminate(10.seconds)
          leaderLatch.close()
          membership.close()
          Behaviors.same
      }

  /**
   * Construct the behavior of the server when it has become the master.
   *
   * @param binding The server binding for the gRPC services.
   * @param leaderLatch The latch for determining the master server.
   * @param membership The [GroupMember] instance for keeping track of the tablet servers via ZooKeeper.
   */
  def master(binding: Http.ServerBinding,
             leaderLatch: LeaderLatch,
             membership: GroupMember): Behavior[HTableServer.Command] =
    Behaviors
      .receiveMessage[HTableServer.Command] {
        case HTableServer.Overthrown =>
          context.log.info("Node was overthrown")
          Behaviors.stopped
      }
      .receiveSignal {
        case (_, PostStop) =>
          context.log.info("HugeTable server stopping")

          binding.terminate(10.seconds)
          leaderLatch.close()
          membership.close()
          Behaviors.same
      }

  /**
   * Create the gRPC services.
   */
  private def createServices(): Future[Http.ServerBinding] = {
    val client = ClientServiceHandler.partial(ClientServiceImpl)
    val admin = AdminServiceHandler.partial(AdminServiceImpl)

    // Create service handlers
    val service: HttpRequest => Future[HttpResponse] =
      ServiceHandler.concatOrNotFound(client, admin)

    // Bind service handler servers to localhost
    Http().bindAndHandleAsync(
      service,
      interface = "127.0.0.1",
      port = 0, // Let the OS assign some port to us.
      connectionContext = HttpConnectionContext()
    )
  }

  private object ClientServiceImpl extends ClientService {

    /**
     * Read the specified row (range) and stream back the response.
     */
    override def read(in: ReadRequest): Source[ReadResponse, NotUsed] = {
      context.log.info("Received READ request")
      Source.single(ReadResponse())
    }

    /**
     * Mutate a specified row in a table.
     */
    override def mutate(in: MutateRequest): Future[MutateResponse] = {
      context.log.info("Received MUTATE request")
      Future.successful(MutateResponse())
    }
  }

  private object AdminServiceImpl extends AdminService {

    /**
     * Create a new table in the cluster.
     */
    override def createTable(in: CreateTableRequest): Future[CreateTableResponse] = ???

    /**
     * Delete a table in the cluster.
     */
    override def deleteTable(in: DeleteTableRequest): Future[DeleteTableResponse] = ???
  }
}
