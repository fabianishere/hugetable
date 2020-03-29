package nl.tudelft.htable.storage.mem

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString
import nl.tudelft.htable.core.{Get, Mutation, Order, Query, Row, RowCell, RowMutation, Scan, Tablet}
import nl.tudelft.htable.storage.TabletDriver

import scala.collection.mutable

/**
 * A [TabletDriver] that stores the memory in a sorted map in memory.
 */
private[mem] class InMemoryTabletDriver(override val tablet: Tablet) extends TabletDriver {

  /**
   * The map storing the cells.
   */
  private val map = new mutable.TreeMap[ByteString, Row]()(Order.keyOrdering)

  /**
   * Perform the specified mutation in the tablet.
   */
  def mutate(mutation: RowMutation): Unit = {
    val row = map.getOrElse(mutation.key, Row(mutation.key, List()))
    val cells: mutable.TreeSet[RowCell] = new mutable.TreeSet()(Order.cellOrdering) ++ row.cells

    for (cellMutation <- mutation.mutations) {
      cellMutation match {
        case Mutation.AppendCell(cell) => cells += cell
        case Mutation.DeleteCell(cell) => cells -= cell
        case Mutation.Delete           => cells.clear()
      }
    }

    map(row.key) = row.copy(cells = cells.toSeq)
  }

  /**
   * Query the specified data in the tablet.
   */
  def read(query: Query): Source[Row, NotUsed] = {
    query match {
      case Get(_, key) =>
        map.get(key) match {
          case Some(value) => Source.single(value)
          case None        => Source.empty
        }
      case Scan(_, range) =>
        Source.fromIterator { () =>
          map
            .iteratorFrom(range.start)
            .takeWhile { case (key, _) => !range.isRightBounded || Order.keyOrdering.compare(key, range.end) < 0 }
            .map(_._2)
        }
    }
  }

  override def close(): Unit = map.clear()
}
