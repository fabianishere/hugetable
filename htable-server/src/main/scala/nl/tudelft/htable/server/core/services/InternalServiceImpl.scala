package nl.tudelft.htable.server.core.services

import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorSystem, DispatcherSelector}
import akka.util.Timeout
import nl.tudelft.htable.protocol.InternalAdapters._
import nl.tudelft.htable.protocol.internal._
import nl.tudelft.htable.server.core.NodeManager

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
 * Implementation of the gRPC [InternalService].
 */
private[htable] class InternalServiceImpl(context: ActorContext[AnyRef])(implicit val sys: ActorSystem[Nothing])
  extends InternalService {
  // asking someone requires a timeout if the timeout hits without response
  // the ask is failed with a TimeoutException
  implicit val timeout: Timeout = 3.second
  implicit val ec: ExecutionContext = sys.dispatchers.lookup(DispatcherSelector.default())

  /**
   * Ping a self in the cluster.
   */
  override def ping(in: PingRequest): Future[PingResponse] = context.self.ask(NodeManager.Ping).map(_ => PingResponse())

  /**
   * QueryTablets a self for the tablets it's serving.
   */
  override def query(in: QueryRequest): Future[QueryResponse] =
    context.self
      .ask(NodeManager.QueryTablets)
      .map(res => QueryResponse(res.tablets.map(t => t)))

  /**
   * Assign the specified tablets to the self.
   */
  override def assign(in: AssignRequest): Future[AssignResponse] = {
    context.self ! NodeManager.Assign(in.tablets.map(t => t))
    Future.successful(AssignResponse())
  }
}
