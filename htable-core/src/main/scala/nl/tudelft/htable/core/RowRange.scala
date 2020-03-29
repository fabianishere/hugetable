package nl.tudelft.htable.core

import akka.util.ByteString

/**
 * A concrete range for [ByteString]s.
 *
 * @param start The start of the range (inclusive).
 * @param end The end of the range (exclusive).
 */
final case class RowRange(start: ByteString, end: ByteString) {

  /**
   * Determine whether this row range is left bounded.
   */
  def isLeftBounded: Boolean = start.nonEmpty

  /**
   * Determine whether this row range is right bounded.
   */
  def isRightBounded: Boolean = end.nonEmpty

  /**
   * Determine whether this row range is unbounded.
   */
  def isUnbounded: Boolean = !(isLeftBounded && isRightBounded)
}

object RowRange {

  /**
   * Obtain an unbounded [RowRange].
   */
  val unbounded: RowRange = RowRange(ByteString.empty, ByteString.empty)

  /**
   * Obtain a [RowRange] that is left bounded (inclusive).
   */
  def leftBounded(start: ByteString): RowRange = RowRange(start, ByteString.empty)

  /**
   * Obtain a [RowRange] that is right bounded (exclusive).
   */
  def rightBounded(end: ByteString): RowRange = RowRange(ByteString.empty, end)
}
