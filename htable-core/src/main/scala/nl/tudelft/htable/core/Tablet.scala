package nl.tudelft.htable.core

/**
 * A tablet is a subset of a table.
 */
final case class Tablet(table: String, range: RowRange) extends Ordered[Tablet] {
  override def compare(that: Tablet): Int = Order.tabletOrdering.compare(this, that)
}

object Tablet {

  /**
   * Obtain the root METADATA tablet.
   */
  val root: Tablet = Tablet("METADATA", RowRange.unbounded)

  /**
   * Determine if a [Tablet] is the root tablet.
   */
  def isRoot(tablet: Tablet): Boolean = tablet.table == "METADATA" && !tablet.range.isLeftBounded
}
