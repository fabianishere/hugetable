package nl.tudelft.htable.client

import akka.util.ByteString

/**
 * A logical row in a HTable table consisting of cells.
 */
case class Row(key: ByteString, cells: Seq[RowCell])
