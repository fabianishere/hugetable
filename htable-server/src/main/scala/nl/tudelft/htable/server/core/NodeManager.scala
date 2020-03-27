package nl.tudelft.htable.server.core

import java.net.InetSocketAddress

import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter.TypedActorSystemOps
import akka.actor.typed._
import akka.grpc.GrpcClientSettings
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import nl.tudelft.htable.core._
import nl.tudelft.htable.protocol.SerializationUtils
import nl.tudelft.htable.protocol.SerializationUtils._
import nl.tudelft.htable.protocol.client.ClientServiceClient
import nl.tudelft.htable.protocol.internal.{AssignRequest, InternalServiceClient, PingRequest, QueryRequest}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success}

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
   * Message sent to a node to query its set of tablets.
   */
  final case class QueryTablets(replyTo: ActorRef[QueryTabletsResponse]) extends Command

  /**
   * Message sent to a node to assign it a set of tablets.
   */
  final case class Assign(tablets: Seq[Tablet]) extends Command

  /**
   * Read the following query from the node.
   */
  final case class Read(query: Query, replyTo: ActorRef[ReadResponse]) extends Command

  /**
   * Mutate the given data on the node.
   */
  final case class Mutate(mutation: RowMutation, replyTo: ActorRef[MutateResponse.type]) extends Command

  /**
   * Events emitted by a [Node].
   */
  sealed trait Event

  /**
   * Message sent when a ping is received.
   */
  final case class Pong(node: Node) extends Event

  /**
   * Message sent in response to a query.
   */
  final case class QueryTabletsResponse(node: Node, tablets: Seq[Tablet]) extends Event

  /**
   * Message sent in response to a read query.
   */
  final case class ReadResponse(rows: Source[Row, NotUsed]) extends Event

  /**
   * Message sent in response to mutation query.
   */
  final case object MutateResponse extends Event

  /**
   * Construct the behavior for the self manager.
   *
   * @param node The node to create the behavior for.
   */
  def apply(node: Node): Behavior[Command] = Behaviors.setup { context =>
    implicit val ec: ExecutionContextExecutor =
      context.system.dispatchers.lookup(DispatcherSelector.default())
    val (client, internalClient) = openClient(node.address)(context.system)

    context.log.info(s"Connecting to node ${node.uid}")

    Behaviors
      .receiveMessage[Command] {
        case Ping(replyTo) =>
          internalClient.ping(PingRequest()).foreach(_ => replyTo ! Pong(node))
          Behaviors.same
        case QueryTablets(replyTo) =>
          internalClient
            .query(QueryRequest())
            .foreach(res => replyTo ! QueryTabletsResponse(node, res.tablets.map(t => t)))
          Behaviors.same
        case Assign(tablets) =>
          internalClient.assign(AssignRequest(tablets = tablets.map(t => t)))
          Behaviors.same
        case Read(query, replyTo) =>
          replyTo ! ReadResponse(SerializationUtils.toRows(client.read(toReadRequest(query))))
          Behaviors.same
        case Mutate(mutation, replyTo) =>
          client.mutate(toMutateRequest(mutation)).onComplete {
            case Success(_)         => replyTo ! NodeManager.MutateResponse
            case Failure(exception) => context.log.error("Failure to run mutation", exception)
          }
          Behaviors.same
      }
      .receiveSignal {
        case (_, PostStop) =>
          internalClient.close()
          Behaviors.same
      }
  }

  /**
   * Open a client for the services endpoint of the nodes.
   */
  private def openClient(address: InetSocketAddress)(
      implicit sys: ActorSystem[Nothing]): (ClientServiceClient, InternalServiceClient) = {
    implicit val classicSystem: akka.actor.ActorSystem = sys.toClassic
    implicit val mat: Materializer = Materializer(sys)
    implicit val ec: ExecutionContext = classicSystem.dispatcher

    val settings = GrpcClientSettings
      .connectToServiceAt(address.getHostString, address.getPort)
      .withTls(false)
    (ClientServiceClient(settings), InternalServiceClient(settings))
  }
}
