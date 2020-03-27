package nl.tudelft.htable.core

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
    .orElseBy(_.startKey.toByteBuffer)
}