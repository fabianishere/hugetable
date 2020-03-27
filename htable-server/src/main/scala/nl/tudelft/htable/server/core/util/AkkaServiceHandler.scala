package nl.tudelft.htable.server.core.util

import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}

import scala.concurrent.Future

/**
 * Temporary fix for https://github.com/akka/akka-grpc/issues/886
 */
private[htable] object AkkaServiceHandler {
  private[htable] val notFound: Future[HttpResponse] = Future.successful(HttpResponse(StatusCodes.NotFound))

  def concatOrNotFound(
      handlers: PartialFunction[HttpRequest, Future[HttpResponse]]*): HttpRequest => Future[HttpResponse] =
    concat(handlers: _*).orElse { case _ => notFound }

  private[htable] def concat(handlers: PartialFunction[HttpRequest, Future[HttpResponse]]*)
    : PartialFunction[HttpRequest, Future[HttpResponse]] =
    handlers.foldLeft(PartialFunction.empty[HttpRequest, Future[HttpResponse]]) {
      case (acc, pf) => acc.orElse(pf)
    }
}
