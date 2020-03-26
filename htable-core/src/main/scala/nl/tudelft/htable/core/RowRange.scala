package nl.tudelft.htable.core

import akka.util.ByteString

/**
 * A concrete range for [ByteString]s.
 */
final case class RowRange(start: ByteString, end: ByteString)
