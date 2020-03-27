package nl.tudelft.htable.server.core

import java.net.InetSocketAddress

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.TypedActorSystemOps
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, DispatcherSelector, PostStop}
import akka.grpc.GrpcClientSettings
import akka.stream.Materializer
import nl.tudelft.htable.core.Node
import nl.tudelft.htable.protocol.internal.{InternalServiceClient, PingRequest}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

/**
 * A self manager manages the connection with nodes in the cluster.
 */
object NodeManager {

  /**
   * Commands that are accepted by the [NodeManager].
   */
  sealed trait Command

  /**
   * Message received when trying to ping the node.
   */
  final case class Ping(replyTo: ActorRef[Pong]) extends Command

  /**
   * Events emitted by a [Node].
   */
  sealed trait Event

  /**
   * Message sent when a ping is received.
   */
  final case class Pong(node: Node) extends Event

  /**
   * Construct the behavior for the self manager.
   *
   * @param node The node to create the behavior for.
   */
  def apply(node: Node): Behavior[Command] = Behaviors.setup { context =>
    implicit val ec: ExecutionContextExecutor =
      context.system.dispatchers.lookup(DispatcherSelector.default())
    val client = openClient(node.address)(context.system)

    context.log.info(s"Connecting to node ${node.uid}")

    Behaviors
      .receiveMessage[Command] {
        case Ping(replyTo) =>
          client.ping(PingRequest()).foreach(_ => replyTo ! Pong(node))
          Behaviors.same
      }
      .receiveSignal {
        case (_, PostStop) =>
          client.close()
          Behaviors.same
      }
  }

  /**
   * Open a client for the internal services endpoint of the nodes.
   */
  private def openClient(address: InetSocketAddress)(implicit sys: ActorSystem[Nothing]): InternalServiceClient = {
    implicit val classicSystem: akka.actor.ActorSystem = sys.toClassic
    implicit val mat: Materializer = Materializer(sys)
    implicit val ec: ExecutionContext = classicSystem.dispatcher

    val settings = GrpcClientSettings
      .connectToServiceAt(address.getHostString, address.getPort)
      .withTls(false)
    InternalServiceClient(settings)
  }
}
