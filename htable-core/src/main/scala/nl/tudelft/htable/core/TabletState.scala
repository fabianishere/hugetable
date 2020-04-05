package nl.tudelft.htable.core

/**
 * The state of a tablet.
 */
object TabletState extends Enumeration {
  type TabletState = Value

  /**
   * The tablet is currently being served by a node.
   */
  val Served: Value = Value

  /**
   * The tablet is currently unassigned and not served by a node.
   */
  val Unassigned: Value = Value

  /**
   * The tablet is closed.
   */
  val Closed: Value = Value
}
