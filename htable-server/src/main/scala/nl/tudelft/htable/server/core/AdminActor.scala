package nl.tudelft.htable.server.core

import akka.Done
import akka.actor.typed.{ActorRef, Behavior, DispatcherSelector}
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import nl.tudelft.htable.client.HTableInternalClient
import nl.tudelft.htable.core.{RowCell, RowMutation, RowRange, Scan, Tablet, TabletState}

import scala.concurrent.{ExecutionContext, Promise}

/**
 * The admin actor handles administrative requests to the cluster, such as creating, deleting or
 * splitting a table.
 */
object AdminActor {
  /**
   * Commands accepted by this actor.
   */
  sealed trait Command

  /**
   * A command to enable the admin commands.
   */
  final case class Enable(client: HTableInternalClient) extends Command

  /**
   * Request the server to create a new table.
   */
  final case class CreateTable(name: String, promise: Promise[Done]) extends Command

  /**
   * Request the server to delete a table.
   */
  final case class DeleteTable(name: String, promise: Promise[Done]) extends Command

  /**
   * Request the server to invalidate the specified tablets..
   */
  final case class Invalidate(tablets: Seq[Tablet], promise: Promise[Done]) extends Command

  /**
   * Events emitted by this actor.
   */
  sealed trait Event

  /**
   * An event to indicate that the specified tablets have been invalidated.
   */
  final case class Invalidated(tablets: Seq[Tablet]) extends Event

  /**
   * Construct the behavior of the admin actor.
   */
  def apply(listener: ActorRef[Event]): Behavior[Command] = disabled(listener)

  /**
   * Construct the behavior of the admin actor when the endpoint is disabled.
   *
   * @param listener The listener to emit events to.
   */
  def disabled(listener: ActorRef[Event]): Behavior[Command] = Behaviors.setup { context =>
    Behaviors.receiveMessagePartial {
      case Enable(client) =>
        context.log.info("Enabling admin endpoint")
        enabled(listener, client)
      case CreateTable(_, promise) =>
        promise.failure(new IllegalStateException("Admin endpoint not enabled for node"))
        Behaviors.same
      case DeleteTable(_, promise) =>
        promise.failure(new IllegalStateException("Admin endpoint not enabled for node"))
        Behaviors.same
      case Invalidate(_, promise) =>
        promise.failure(new IllegalStateException("Admin endpoint not enabled for node"))
        Behaviors.same
    }
  }

  /**
   * Construct the behavior of the admin actor when the endpoint is enabled.
   *
   * @param listener The listener to emit events to.
   * @param client The client to communicate with other nodes.
   */
  def enabled(listener: ActorRef[Event], client: HTableInternalClient): Behavior[Command] = Behaviors.setup { context =>
    implicit val mat: Materializer = Materializer(context.system)
    implicit val ec: ExecutionContext = context.system.dispatchers.lookup(DispatcherSelector.default())
    Behaviors.receiveMessagePartial {
      case Enable(client) =>
        context.log.debug("Re-enabling admin actor")
        enabled(listener, client)
      case CreateTable(table, promise) =>
        context.log.info(s"Creating new table $table")
        val time = System.currentTimeMillis()
        val mutation = RowMutation("METADATA", ByteString(table))
          .put(RowCell(ByteString("table"), time, ByteString(table)))
          .put(RowCell(ByteString("start-key"), time, ByteString.empty))
          .put(RowCell(ByteString("end-key"), time, ByteString.empty))
          .put(RowCell(ByteString("state"), time, ByteString(TabletState.Unassigned.id)))
        client.mutate(mutation).onComplete(promise.complete)
        listener ! Invalidated(Seq.empty)
        Behaviors.same
      case DeleteTable(table, promise) =>
        context.log.info(s"Deleting table $table")

        if (table.equalsIgnoreCase("METADATA")) {
          promise.failure(new IllegalArgumentException("Refusing to remove METADATA"))
        } else {
          client.read(Scan("METADATA", RowRange.leftBounded(ByteString(table))))
            .takeWhile { row =>
              row.cells
                .find(_.qualifier == ByteString("table"))
                .map(_.value.utf8String)
                .contains(table)
            }
            .flatMapConcat { row =>
              val mutation = RowMutation("METADATA", row.key)
                .delete()
              Source.future(client.mutate(mutation))
            }
            .runWith(Sink.onComplete(res => {
              if (res.isFailure) {
                context.log.error("Failed to complete removal", res.failed.get)
              }
              listener ! Invalidated(Seq.empty)
              promise.complete(res)
            }))
        }
        Behaviors.same
      case Invalidate(tablets, promise) =>
        listener ! Invalidated(tablets)
        promise.success(Done)
        Behaviors.same
    }
  }
}
