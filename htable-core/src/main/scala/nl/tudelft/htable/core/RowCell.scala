package nl.tudelft.htable.core

import java.nio.ByteBuffer

import akka.util.ByteString

/**
 * A cell within a [Row].
 */
final case class RowCell(qualifier: ByteString, timestamp: Long, value: ByteString) extends Ordered[RowCell] {
  override def compare(that: RowCell): Int = RowCell.ordering.compare(this, that)
}

object RowCell {
  /**
   * The default ordering of a row cell.
   */
  private val ordering: Ordering[RowCell] = Ordering
    .by[RowCell, ByteBuffer](_.qualifier.toByteBuffer)
    .orElseBy(_.timestamp)
}
