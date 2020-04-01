package nl.tudelft.htable.server.core.services

import akka.Done
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorSystem, DispatcherSelector}
import akka.util.Timeout
import nl.tudelft.htable.protocol.admin._
import nl.tudelft.htable.server.core.HTableServer

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
 * Implementation of the gRPC [AdminService].
 */
private[htable] class AdminServiceImpl(context: ActorContext[AnyRef])(implicit val sys: ActorSystem[Nothing])
    extends AdminService {
  implicit val timeout: Timeout = 3.seconds
  implicit val ec: ExecutionContext = sys.dispatchers.lookup(DispatcherSelector.default())

  /**
   * Create a new table in the cluster.
   */
  override def createTable(in: CreateTableRequest): Future[CreateTableResponse] =
    context.self
      .ask[Done](ref => HTableServer.CreateTable(in.tableName, ref))
      .map(_ => CreateTableResponse())

  /**
   * Delete a table in the cluster.
   */
  override def deleteTable(in: DeleteTableRequest): Future[DeleteTableResponse] =
    context.self
      .ask[Done](ref => HTableServer.DeleteTable(in.tableName, ref))
      .map(_ => DeleteTableResponse())
}
