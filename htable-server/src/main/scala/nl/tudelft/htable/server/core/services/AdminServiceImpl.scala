package nl.tudelft.htable.server.core.services

import akka.Done
import akka.actor.typed.{ActorRef, ActorSystem, DispatcherSelector}
import akka.util.Timeout
import nl.tudelft.htable.core.{RowRange, Tablet}
import nl.tudelft.htable.protocol.CoreAdapters._
import nl.tudelft.htable.protocol.admin._
import nl.tudelft.htable.server.core.{AdminActor, HTableActor}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
 * Implementation of the gRPC [AdminService].
 */
private[htable] class AdminServiceImpl(handler: ActorRef[AdminActor.Command])(implicit val sys: ActorSystem[Nothing])
    extends AdminService {
  implicit val timeout: Timeout = 3.seconds
  implicit val ec: ExecutionContext = sys.dispatchers.lookup(DispatcherSelector.default())

  /**
   * Create a new table in the cluster.
   */
  override def createTable(in: CreateTableRequest): Future[CreateTableResponse] = {
    val promise = Promise[Done]
    handler ! AdminActor.CreateTable(in.tableName, promise)
    promise.future.map(_ => CreateTableResponse())
  }

  /**
   * Delete a table in the cluster.
   */
  override def deleteTable(in: DeleteTableRequest): Future[DeleteTableResponse] = {
    val promise = Promise[Done]
    handler ! AdminActor.DeleteTable(in.tableName, promise)
    promise.future.map(_ => DeleteTableResponse())
  }

  /**
   * Split a table in the cluster.
   */
  override def splitTable(in: SplitTableRequest): Future[SplitTableResponse] = {
    val promise = Promise[Done]
    handler ! AdminActor.SplitTable(Tablet(in.tableName, RowRange.leftBounded(in.startKey)), in.splitKey, promise)
    promise.future.map(_ => SplitTableResponse())
  }
}
