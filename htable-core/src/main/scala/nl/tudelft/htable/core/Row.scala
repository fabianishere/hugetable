package nl.tudelft.htable.core

import akka.util.ByteString

/**
 * A logical row in a HTable table consisting of cells.
 */
final case class Row(key: ByteString, cells: Seq[RowCell])
