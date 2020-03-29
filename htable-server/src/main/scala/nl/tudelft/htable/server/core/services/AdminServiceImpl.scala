package nl.tudelft.htable.server.core.services

import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.ActorSystem
import nl.tudelft.htable.protocol.admin.{AdminService, CreateTableRequest, CreateTableResponse, DeleteTableRequest, DeleteTableResponse}

import scala.concurrent.Future

/**
 * Implementation of the gRPC [AdminService].
 */
private[htable] class AdminServiceImpl(context: ActorContext[AnyRef])(implicit val sys: ActorSystem[Nothing])
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
