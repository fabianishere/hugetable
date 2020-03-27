package nl.tudelft.htable.core

import akka.util.ByteString

/**
 * A logical row in a HTable table consisting of cells.
 */
final case class Row(key: ByteString, cells: Seq[RowCell]) extends Ordered[Row] {
  override def compare(that: Row): Int = Row.ordering.compare(this, that)
}

object Row {

  /**
   * The default ordering of a row.
   */
  private val ordering: Ordering[Row] = Ordering.by(_.key.toByteBuffer)
}
