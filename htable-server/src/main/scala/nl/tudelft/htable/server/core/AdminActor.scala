package nl.tudelft.htable.server.core

import akka.Done
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.util.ByteString
import nl.tudelft.htable.client.HTableInternalClient
import nl.tudelft.htable.core.Tablet

import scala.concurrent.Promise

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
   * Request the server to split a tablet.
   */
  final case class SplitTable(tablet: Tablet, splitKey: ByteString, promise: Promise[Done]) extends Command

  /**
   * Construct the behavior of the admin actor.
   */
  def apply(): Behavior[Command] = disabled()

  /**
   * Construct the behavior of the admin actor when the endpoint is disabled.
   */
  def disabled(): Behavior[Command] = Behaviors.setup { context =>
    Behaviors.receiveMessagePartial {
      case Enable(client) =>
        context.log.info("Enabling admin endpoint")
        enabled(client)
      case CreateTable(_, promise) =>
        promise.failure(new IllegalStateException("Admin endpoint not enabled for node"))
        Behaviors.same
      case DeleteTable(_, promise) =>
        promise.failure(new IllegalStateException("Admin endpoint not enabled for node"))
        Behaviors.same
      case SplitTable(_, _, promise) =>
        promise.failure(new IllegalStateException("Admin endpoint not enabled for node"))
        Behaviors.same
    }
  }

  /**
   * Construct the behavior of the admin actor when the endpoint is enabled.
   *
   * @param client The client to communicate with other nodes.
   */
  def enabled(client: HTableInternalClient): Behavior[Command] = Behaviors.setup { context =>
    Behaviors.receiveMessagePartial {
      case Enable(client) =>
        context.log.debug("Re-enabling admin actor")
        enabled(client)
      case CreateTable(_, promise) =>
        promise.failure(new NotImplementedError())
        Behaviors.same
      case DeleteTable(_, promise) =>
        promise.failure(new NotImplementedError())
        Behaviors.same
      case SplitTable(_, _, promise) =>
        promise.failure(new NotImplementedError())
        Behaviors.same
    }
  }
}
