package nl.tudelft.htable.core

import java.util.Comparator

import akka.util.ByteString

/**
 * A tablet is a subset of a table.
 */
final case class Tablet(table: String, startKey: ByteString, endKey: ByteString) extends Ordered[Tablet] {
  override def compare(that: Tablet): Int = Tablet.ordering.compare(this, that)
}

object Tablet {

  /**
   * The default ordering of a tablet.
   */
  private val ordering: Ordering[Tablet] = Ordering
    .by[Tablet, String](_.table)
    .orElse(Ordering.comparatorToOrdering(new Comparator[Tablet] {
      def compare(l: Tablet, r: Tablet): Int = {

        var result = l.startKey.toByteBuffer.compareTo(r.startKey.toByteBuffer)
        if (result != 0) {
          return result
        }

        result = r.endKey.toByteBuffer.compareTo(r.endKey.toByteBuffer)

        if (l.startKey.nonEmpty && l.endKey.isEmpty) {
          1 // This is the last region
        } else if (r.startKey.nonEmpty && r.endKey.isEmpty) {
          -1 // r is the last region
        } else {
          result
        }
      }
    }))

  /**
   * Obtain the root METADATA tablet.
   */
  val root: Tablet = Tablet("METADATA", ByteString.empty, ByteString.empty)

  /**
   * Determine if a [Tablet] is the root tablet.
   */
  def isRoot(tablet: Tablet): Boolean = tablet.table == "METADATA" && isUnbounded(tablet.startKey)

  /**
   * Determine whether the key is unbounded.
   */
  def isUnbounded(key: ByteString): Boolean = key.isEmpty
}
