package nl.tudelft.htable.server

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, DispatcherSelector, PostStop}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import akka.{Done, NotUsed}
import nl.tudelft.htable.client.HTableInternalClient
import nl.tudelft.htable.client.impl.MetaHelpers
import nl.tudelft.htable.core.TabletState.TabletState
import nl.tudelft.htable.core._
import nl.tudelft.htable.server.AdminActor.{Command, CreateTable, DeleteTable, Enable, Event, Invalidate, enabled}
import nl.tudelft.htable.storage.{StorageDriver, StorageDriverProvider, TabletDriver}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Promise}
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
   * A command to enable the node commands.
   */
  final case class Enable(client: HTableInternalClient) extends Command

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
   * An event to indicate that the node is now serving the specified tablets.
   */
  final case class Serving(newTablets: Seq[Tablet], removedTablets: Seq[Tablet]) extends Event

  /**
   * Construct the behavior of the node actor.
   *
   * @param self The node that we represent.
   * @param sdp The driver to use for accessing the data storage.
   * @param listener The listener to emit events to.
   */
  def apply(self: Node, sdp: StorageDriverProvider, listener: ActorRef[Event]): Behavior[Command] =
    disabled(self, sdp, listener)

  /**
   * Construct the behavior of the node actor when the endpoint is disabled.
   *
   * @param self The node that we represent.
   * @param sdp The driver to use for accessing the data storage.
   * @param listener The listener to emit events to.
   */
  def disabled(self: Node, sdp: StorageDriverProvider, listener: ActorRef[Event]): Behavior[Command] = Behaviors.setup { context =>
    Behaviors.receiveMessagePartial {
      case Enable(client) =>
        context.log.info("Enabling node endpoint")
        enabled(self, sdp, listener, client)
      case Ping(promise) =>
        promise.success(Done)
        Behaviors.same
      case Report(promise) =>
        promise.success(Seq.empty)
        Behaviors.same
      case Assign(_, promise) =>
        promise.failure(new IllegalStateException(s"Node endpoint not enabled for node $self"))
        Behaviors.same
      case Read(_, promise) =>
        promise.failure(new NotServingTabletException(s"The tablet is not served on this node"))
        Behaviors.same
      case Mutate(_, promise) =>
        promise.failure(new NotServingTabletException(s"The tablet is not served on this node"))
        Behaviors.same
      case Split(_, _, promise) =>
        promise.failure(new NotServingTabletException(s"The tablet is not served on this node"))
        Behaviors.same
    }
  }
  /**
   * Construct the behavior of an enabled node actor.
   *
   * @param self The node that we represent.
   * @param sdp The driver to use for accessing the data storage.
   * @param listener The listener to emit events to.
   */
  def enabled(self: Node,
              sdp: StorageDriverProvider,
              listener: ActorRef[Event],
              client: HTableInternalClient): Behavior[Command] = Behaviors.setup {
    context =>
      implicit val ec: ExecutionContext = context.system.dispatchers.lookup(DispatcherSelector.blocking())
      context.log.info(s"Starting actor for node $self")
      val storageDriver = sdp.create(self)
      val tablets = new mutable.TreeMap[Tablet, TabletDriver]()

      /**
       * Find the tablet closest to the given key.
       */
      def find(table: String, key: ByteString): Option[(Tablet, TabletDriver)] = {
        val nextKey = key ++ ByteString(0) // Ensure that our key is strictly greater
        tablets
          .rangeTo(Tablet(table, RowRange.leftBounded(nextKey)))
          .dropWhile(_._1.table != table)
          .lastOption
      }

      Behaviors
        .receiveMessagePartial[Command] {
          case Ping(promise) =>
            promise.success(Done)
            Behaviors.same
          case Report(promise) =>
            promise.success(tablets.keys.toSeq)
            Behaviors.same
          case Assign(newTablets, promise) =>
            val newTabletsSet = newTablets.toSet
            val removeTablets = tablets.keySet.diff(newTabletsSet)
            val addTablets = newTabletsSet.diff(tablets.keySet)

            // Remove tablets
            for (tablet <- removeTablets) {
              val driver = tablets.remove(tablet)
              driver.foreach(_.close())
            }

            // Spawn new tablet managers
            for (tablet <- addTablets) {
              tablets.put(tablet, storageDriver.createTablet(tablet))
            }

            listener ! Serving(addTablets.toSeq, removeTablets.toSeq)

            promise.success(Done)
            Behaviors.same
          case Read(query, promise) =>
            context.log.debug(s"READ $query")
            query match {
              case Get(table, key) =>
                find(table, key) match {
                  case Some((_, driver)) => promise.success(driver.read(query))
                  case None              => promise.failure(NotServingTabletException(s"The key $key is not served"))
                }
              case Scan(table, range, reversed) =>
                find(table, range.start) match {
                  case Some((start, _)) =>
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
            context.log.debug(s"MUTATE $mutation")

            find(mutation.table, mutation.key) match {
              case Some((_, driver)) =>
                promise.complete(Try { driver.mutate(mutation) }.map(_ => Done))
              case None =>
                promise.failure(NotServingTabletException(s"The key ${mutation.key} is not served"))
            }
            Behaviors.same
          case Split(tablet, splitKey, promise) =>
            context.log.debug(s"Split tablet $tablet at $splitKey")
            find(tablet.table, tablet.range.start) match {
              case Some((tablet, driver)) =>
                try {
                  val (left, right) = driver.split(splitKey)
                  promise.completeWith(for {
                    _ <- client.mutate(MetaHelpers.writeExisting(tablet, TabletState.Closed, None))
                    _ <- client.mutate(MetaHelpers.writeNew(left, TabletState.Unassigned, None))
                    _ <- client.mutate(MetaHelpers.writeNew(right, TabletState.Unassigned, None))
                    _ <- client.invalidate(Seq(tablet, left, right))
                  } yield Done)
                } catch {
                  case e: Throwable => promise.failure(e)
                }
              case None =>
                promise.failure(NotServingTabletException(s"The tablet $tablet is not served"))
            }
            Behaviors.same
        }
        .receiveSignal {
          case (_, PostStop) =>
            tablets.foreach(_._2.close())
            storageDriver.close()
            Behaviors.stopped
        }
  }
}
