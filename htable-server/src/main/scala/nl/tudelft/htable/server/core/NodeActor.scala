package nl.tudelft.htable.server.core

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import nl.tudelft.htable.core._
import nl.tudelft.htable.storage.{StorageDriver, TabletDriver}

import scala.collection.mutable
import scala.concurrent.Promise
import scala.util.Try

/**
 * Actor that models the behavior of a tablet server that serves the tablet data.
 */
object NodeActor {

  /**
   * Commands that are accepted by the [NodeActor].
   */
  sealed trait Command

  /**
   * Message received when trying to ping the node.
   */
  final case class Ping(promise: Promise[Done]) extends Command

  /**
   * Message sent to a node to query its set of tablets.
   */
  final case class Report(promise: Promise[Seq[Tablet]]) extends Command

  /**
   * Message sent to a node to assign it a set of tablets.
   */
  final case class Assign(tablets: Seq[Tablet], promise: Promise[Done]) extends Command

  /**
   * Read the following query from the node.
   */
  final case class Read(query: Query, promise: Promise[Source[Row, NotUsed]]) extends Command

  /**
   * Mutate the given data on the node.
   */
  final case class Mutate(mutation: RowMutation, promise: Promise[Done]) extends Command

  /**
   * Construct the behavior of the node actor.
   *
   * @param self The node that we represent.
   * @param storageDriver The driver to use for accessing the data storage.
   */
  def apply(self: Node, storageDriver: StorageDriver): Behavior[Command] = Behaviors.setup { context =>
    context.log.info(s"Starting actor for node $self")

    val tablets = new mutable.TreeMap[Tablet, TabletDriver]()

    Behaviors.receiveMessagePartial {
      case Ping(promise) =>
        promise.success(Done)
        Behaviors.same
      case Report(promise) =>
        promise.success(tablets.keys.toSeq)
        Behaviors.same
      case Assign(newTablets, promise) =>
        tablets.foreach { case (_, driver) => driver.close() }
        tablets.clear()

        // Spawn new tablet managers
        for (tablet <- newTablets) {
          tablets.put(tablet, storageDriver.openTablet(tablet))
        }

        promise.success(Done)
        Behaviors.same
      case Read(query, promise) =>
        context.log.debug(s"READ $query")
        query match {
          case Get(table, key) =>
            tablets.rangeTo(Tablet(table, RowRange.leftBounded(key))).lastOption match {
              case Some((_, driver)) => promise.success(driver.read(query))
              case None              => promise.failure(NotServingTabletException(s"The key $key is not served"))
            }
          case Scan(table, range, reversed) =>
            val start = Tablet(table, RowRange.leftBounded(range.start))
            val end = Tablet(table, RowRange.leftBounded(range.end))
            val submap = if (range.isUnbounded) tablets.rangeTo(end) else tablets.rangeUntil(end)

            val source: Source[Row, NotUsed] =
              Source(if (reversed) submap.toSeq.reverse else submap.toSeq)
                .takeWhile({ case (tablet, _) => Order.tabletOrdering.gt(tablet, start) }, inclusive = true)
                .flatMapConcat { case (_, driver) => driver.read(query) }
            promise.success(source)
        }
        Behaviors.same
      case Mutate(mutation, promise) =>
        context.log.debug(s"MUTATE $mutation")

        tablets.rangeTo(Tablet(mutation.table, RowRange.leftBounded(mutation.key))).lastOption match {
          case Some((_, driver)) => promise.complete(Try { driver.mutate(mutation) }.map(_ => Done))
          case None              => promise.failure(NotServingTabletException(s"The key ${mutation.key} is not served"))
        }
        Behaviors.same
    }
  }
}
