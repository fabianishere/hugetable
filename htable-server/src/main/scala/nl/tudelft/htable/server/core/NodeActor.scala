package nl.tudelft.htable.server.core

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import akka.{Done, NotUsed}
import nl.tudelft.htable.core.TabletState.TabletState
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
   * Request the server to split a tablet.
   */
  final case class Split(tablet: Tablet, splitKey: ByteString, promise: Promise[Done]) extends Command

  /**
   * Events emitted by the [NodeActor].
   */
  sealed trait Event

  /**
   * An event to indicate that the specified tablets have been invalidated.
   */
  final case class Invalidated(tablets: Map[Tablet, TabletState]) extends Event

  /**
   * Construct the behavior of the node actor.
   *
   * @param self The node that we represent.
   * @param storageDriver The driver to use for accessing the data storage.
   * @param listener The listener to emit events to.
   */
  def apply(self: Node, storageDriver: StorageDriver, listener: ActorRef[Event]): Behavior[Command] = Behaviors.setup {
    context =>
      context.log.info(s"Starting actor for node $self")

      val tablets = new mutable.TreeMap[Tablet, TabletDriver]()

      /**
       * Find the tablet closest to the given key.
       */
      def find(table: String, key: ByteString): Option[(Tablet, TabletDriver)] = {
        tablets
          .rangeTo(Tablet(table, RowRange.leftBounded(key)))
          .dropWhile(_._1.table != table)
          .lastOption
      }

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
          context.log.info(s"READ $query")
          query match {
            case Get(table, key) =>
              find(table, key) match {
                case Some((_, driver)) => promise.success(driver.read(query))
                case None              => promise.failure(NotServingTabletException(s"The key $key is not served"))
              }
            case Scan(table, range, reversed) =>
              find(table, range.start).map(_._1) match {
                case Some(start) =>
                  val end = Tablet(table, RowRange.leftBounded(range.end))
                  val submap =
                    if (range.isUnbounded) tablets.rangeFrom(start).rangeTo(end) else tablets.range(start, end)
                  val source: Source[Row, NotUsed] =
                    Source(if (reversed) submap.toSeq.reverse else submap.toSeq)
                      .flatMapConcat { case (_, driver) => driver.read(query) }
                  promise.success(source)
                case None =>
                  promise.failure(new IllegalArgumentException("Start key not in range"))
              }
          }
          Behaviors.same
        case Mutate(mutation, promise) =>
          context.log.info(s"MUTATE $mutation")

          find(mutation.table, mutation.key) match {
            case Some((_, driver)) =>
              promise.complete(Try { driver.mutate(mutation) }.map(_ => Done))
            case None =>
              promise.failure(NotServingTabletException(s"The key ${mutation.key} is not served"))
          }
          Behaviors.same
        case Split(tablet, splitKey, promise) =>
          context.log.info(s"Split tablet $tablet at $splitKey")
          find(tablet.table, tablet.range.start) match {
            case Some((tablet, driver)) =>
              promise.complete(Try {
                val (left, right) = driver.split(splitKey)

                val invalidations = new mutable.HashMap[Tablet, TabletState]
                invalidations(tablet) = TabletState.Closed
                invalidations(left) = TabletState.Unassigned
                invalidations(right) = TabletState.Unassigned

                listener ! Invalidated(invalidations.toMap)

                Done
              })
            case None =>
              promise.failure(NotServingTabletException(s"The tablet $tablet is not served"))
          }
          Behaviors.same
      }
  }
}
