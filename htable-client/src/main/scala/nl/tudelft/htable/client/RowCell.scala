package nl.tudelft.htable.client

import akka.util.ByteString

/**
 * A cell within a [Row].
 */
case class RowCell(qualifier: ByteString, timestamp: Long, value: ByteString)
