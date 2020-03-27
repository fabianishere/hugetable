package nl.tudelft.htable.server.core

import akka.util.ByteString

/**
 * A key that indexes a tablet based on table name and start key.
 */
final case class TabletKey(table: String, startKey: ByteString) extends Comparable[TabletKey] {
  override def compareTo(o: TabletKey): Int = {
    val cmp = table.compareTo(o.table)
    if (cmp == 0) {
      startKey.toByteBuffer.compareTo(o.startKey.toByteBuffer)
    } else {
      cmp
    }
  }
}
