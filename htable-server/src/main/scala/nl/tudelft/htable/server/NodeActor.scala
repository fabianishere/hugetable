package nl.tudelft.htable.server

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, DispatcherSelector, PostStop}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import akka.{Done, NotUsed}
import nl.tudelft.htable.client.HTableInternalClient
import nl.tudelft.htable.client.impl.MetaHelpers
import nl.tudelft.htable.core.AssignType.AssignType
import nl.tudelft.htable.core._
import nl.tudelft.htable.storage.{StorageDriverProvider, TabletDriver}

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
  final case class Assign(tablets: Seq[Tablet], assignType: AssignType, promise: Promise[Done]) extends Command

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
  final case class Serving(newTablets: Set[Tablet], removedTablets: Set[Tablet]) extends Event

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
        context.log.warn("Pinging disabled node")
        promise.success(Done)
        Behaviors.same
      case Report(promise) =>
        context.log.warn("Reporting disabled node")
        promise.success(Seq.empty)
        Behaviors.same
      case Assign(_, _, promise) =>
        context.log.warn("Assigning on disabled node")
        promise.failure(new IllegalStateException(s"Node endpoint not enabled for node $self"))
        Behaviors.same
      case Read(_, promise) =>
        context.log.warn("Reading on disabled node")
        promise.failure(new NotServingTabletException(s"The tablet is not served on this node"))
        Behaviors.same
      case Mutate(_, promise) =>
        context.log.warn("Mutating on disabled node")
        promise.failure(new NotServingTabletException(s"The tablet is not served on this node"))
        Behaviors.same
      case Split(_, _, promise) =>
        context.log.warn("Splitting on disabled node")
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
      val tables = new mutable.HashMap[String, mutable.TreeMap[ByteString, TabletDriver]]()

      /**
       * Find the tablet closest to the given key.
       */
      def find(table: String, key: ByteString): Option[TabletDriver] = {
        tables.get(table)
          .flatMap(tablets => tablets.rangeTo(key).lastOption)
          .map(_._2)
      }

      Behaviors
        .receiveMessagePartial[Command] {
          case Ping(promise) =>
            promise.success(Done)
            Behaviors.same
          case Report(promise) =>
            promise.success(tables.flatMap(_._2.values).map(_.tablet).toSeq)
            Behaviors.same
          case Assign(newTablets, assignType, promise) =>
            val currentTablets = tables.flatMap(_._2.values).map(_.tablet).toSet
            val newTabletsSet = newTablets.toSet
            val removeTablets =
              if (assignType == AssignType.Set)
                currentTablets.diff(newTabletsSet)
              else
                Set.empty[Tablet]
            val addTablets = newTabletsSet.diff(currentTablets)

            // Remove tablets
            for (tablet <- removeTablets) {
              context.log.info(s"REMOVE: $tablet from $self")
              val driver = tables.get(tablet.table).flatMap(_.remove(tablet.range.start))
              driver.foreach(_.close())
            }

            // Spawn new tablet managers
            for (tablet <- addTablets) {
              context.log.info(s"SPAWN: $tablet on $self")
              val tablets = tables.getOrElseUpdate(tablet.table, new mutable.TreeMap[ByteString, TabletDriver]()(Order.keyOrdering))
              tablets.put(tablet.range.start, storageDriver.openTablet(tablet))
            }

            // Inform root actor if changes
            if (addTablets.nonEmpty || removeTablets.nonEmpty) {
              listener ! Serving(addTablets, removeTablets)
            }

            promise.success(Done)
            Behaviors.same
          case Read(query, promise) =>
            context.log.debug(s"READ: $query on $self")
            query match {
              case Get(table, key) =>
                find(table, key) match {
                  case Some(driver) =>
                    context.log.trace(s"READ: Asking ${driver.tablet} for $key in $table")
                    promise.success(driver.read(query))
                  case None              =>
                    context.log.debug(s"READ: Unknown $key in $table")
                    promise.failure(NotServingTabletException(s"The key $key is not served"))
                }
              case Scan(table, range, reversed) =>
                find(table, range.start) match {
                  case Some(startDriver) =>
                    val tablets = tables(table)
                    val submap =
                      if (range.isUnbounded) {
                        tablets
                      } else if (!range.isRightBounded) {
                        tablets.rangeFrom(startDriver.tablet.range.start)
                      } else {
                        tablets.range(startDriver.tablet.range.start, range.end)
                      }

                    context.log.trace(s"READ: Asking $submap for $range in $table")
                    val source: Source[Row, NotUsed] =
                      Source(if (reversed) submap.toSeq.reverse else submap.toSeq)
                        .flatMapConcat { case (_, driver) => driver.read(query) }
                    promise.success(source)
                  case None =>
                    context.log.debug(s"READ: Unknown $range in $table")
                    promise.failure(new IllegalArgumentException("Start key not in range"))
                }
            }
            Behaviors.same
          case Mutate(mutation, promise) =>
            context.log.debug(s"MUTATE: $mutation on $self")

            find(mutation.table, mutation.key) match {
              case Some(driver) =>
                context.log.trace(s"MUTATE: Asking ${driver.tablet} for ${mutation.key} in ${mutation.table}")
                promise.complete(Try { driver.mutate(mutation) }.map(_ => Done))
              case None =>
                context.log.debug(s"MUTATE: Unknown ${mutation.key} in ${mutation.table}")
                promise.failure(NotServingTabletException(s"The key ${mutation.key} is not served"))
            }
            Behaviors.same
          case Split(tablet, splitKey, promise) =>
            context.log.debug(s"SPLIT: $tablet at $splitKey on $self")
            find(tablet.table, tablet.range.start) match {
              case Some(driver) =>
                try {
                  context.log.trace(s"SPLIT: Asking ${driver.tablet} for ${splitKey} in ${tablet.table}")

                  val (left, right) = driver.split(splitKey)
                  promise.completeWith(for {
                    _ <- client.mutate(MetaHelpers.writeExisting(tablet, TabletState.Closed, None))
                    _ <- client.mutate(MetaHelpers.writeNew(left, TabletState.Unassigned, None))
                    _ <- client.mutate(MetaHelpers.writeNew(right, TabletState.Unassigned, None))
                    _ <- client.balance(Set(tablet, left, right))
                  } yield Done)
                } catch {
                  case e: Throwable =>
                    context.log.warn("SPLIT: Failure during split", e)
                    promise.failure(e)
                }
              case None =>
                context.log.debug(s"SPLIT: Unknown ${tablet.range} in ${tablet.table}")
                promise.failure(NotServingTabletException(s"The tablet $tablet is not served"))
            }
            Behaviors.same
        }
        .receiveSignal {
          case (_, PostStop) =>
            tables.flatMap(_._2).foreach(_._2.close())
            storageDriver.close()
            Behaviors.stopped
        }
  }
}
