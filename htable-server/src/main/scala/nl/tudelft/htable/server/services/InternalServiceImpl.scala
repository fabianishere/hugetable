package nl.tudelft.htable.server.services

import akka.Done
import akka.actor.typed.{ActorRef, ActorSystem, DispatcherSelector}
import akka.util.Timeout
import nl.tudelft.htable.core.{AssignType, Tablet}
import nl.tudelft.htable.protocol.internal._
import nl.tudelft.htable.server.NodeActor

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
 * Implementation of the gRPC [InternalService].
 */
private[htable] class InternalServiceImpl(handler: ActorRef[NodeActor.Command])(implicit val sys: ActorSystem[Nothing])
    extends InternalService {
  // asking someone requires a timeout if the timeout hits without response
  // the ask is failed with a TimeoutException
  implicit val timeout: Timeout = 3.second
  implicit val ec: ExecutionContext = sys.dispatchers.lookup(DispatcherSelector.default())

  /**
   * Ping a node in the cluster.
   */
  override def ping(in: PingRequest): Future[PingResponse] = {
    val promise = Promise[Done]
    handler ! NodeActor.Ping(promise)
    promise.future.map(_ => PingResponse())
  }

  /**
   * Query a node for the tablets it's serving.
   */
  override def report(in: ReportRequest): Future[ReportResponse] = {
    val promise = Promise[Seq[Tablet]]
    handler ! NodeActor.Report(promise)
    promise.future.map(tablets => ReportResponse(tablets))
  }

  /**
   * Assign the specified tablets to the node.
   */
  override def setTablets(in: SetTabletsRequest): Future[SetTabletsResponse] = {
    val promise = Promise[Done]
    handler ! NodeActor.Assign(in.tablets, AssignType.Set, promise)
    promise.future.map(_ => SetTabletsResponse())
  }

  /**
   * Assign the specified tablets to the node.
   */
  override def addTablets(in: AddTabletsRequest): Future[AddTabletsResponse] = {
    val promise = Promise[Done]
    handler ! NodeActor.Assign(in.tablets, AssignType.Add, promise)
    promise.future.map(_ => AddTabletsResponse())
  }

  /**
   * Perform a split of the specified table.
   */
  override def split(in: SplitRequest): Future[SplitResponse] = {
    val promise = Promise[Done]
    in.tablet match {
      case Some(value) =>
        handler ! NodeActor.Split(value, in.splitKey, promise)
      case None =>
        promise.failure(new IllegalArgumentException())
    }
    promise.future.map(_ => SplitResponse())
  }
}
