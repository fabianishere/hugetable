package nl.tudelft.htable.server.core.services

import akka.NotUsed
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.{ActorSystem, DispatcherSelector}
import akka.stream.scaladsl.Source
import akka.util.Timeout
import nl.tudelft.htable.protocol.ClientAdapters
import nl.tudelft.htable.protocol.ClientAdapters._
import nl.tudelft.htable.protocol.client.{ClientService, MutateRequest, MutateResponse, ReadRequest, ReadResponse}
import nl.tudelft.htable.server.core.NodeManager

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
 * Implementation of the gRPC [ClientService].
 */
private[htable] class ClientServiceImpl(context: ActorContext[AnyRef])(implicit val sys: ActorSystem[Nothing])
  extends ClientService {
  implicit val timeout: Timeout = 3.seconds
  implicit val ec: ExecutionContext = sys.dispatchers.lookup(DispatcherSelector.default())

  /**
   * Read the specified row (range) and stream back the response.
   */
  override def read(in: ReadRequest): Source[ReadResponse, NotUsed] = {
    Source
      .future(context.self.ask[NodeManager.ReadResponse](ref => NodeManager.Read(in, ref)))
      .flatMapConcat(_.rows)
      .map(row => ReadResponse(cells = row.cells.map(cell => ClientAdapters.cellToProtobuf(row, cell))))
  }

  /**
   * Mutate a specified row in a table.
   */
  override def mutate(in: MutateRequest): Future[MutateResponse] = {
    context.self
      .ask[NodeManager.MutateResponse.type](NodeManager.Mutate(in, _))
      .map(_ => MutateResponse())
  }
}
