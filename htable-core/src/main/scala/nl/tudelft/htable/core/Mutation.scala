package nl.tudelft.htable.core

/**
 * A mutation within a single row.
 */
sealed trait Mutation

object Mutation {

  /**
   * Append a cell to the row.
   */
  final case class AppendCell(cell: RowCell) extends Mutation

  /**
   * Remove a cell from the row.
   */
  final case class DeleteCell(cell: RowCell) extends Mutation

  /**
   * Remove the entire row.
   */
  final case object Delete extends Mutation
}
