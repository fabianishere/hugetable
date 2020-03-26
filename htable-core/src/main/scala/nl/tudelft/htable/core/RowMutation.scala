package nl.tudelft.htable.core

import scala.collection.immutable.ArraySeq

/**
 * A builder class for constructing a row mutation.
 *
 * @param table The table in which to mutate a row.
 * @param key The key of the row to mutate.
 */
final case class RowMutation private (table: String, key: ArraySeq[Byte], mutations: List[Mutation]) {

  /**
   * Append the specified cell to the row.
   */
  def append(cell: RowCell): RowMutation = copy(mutations = Mutation.AppendCell(cell) :: mutations)

  /**
   * Mark the row as deleted.
   */
  def delete(): RowMutation = copy(mutations = Mutation.Delete :: mutations)

  /**
   * Mark a cell from the row as deleted.
   */
  def delete(cell: RowCell): RowMutation = copy(mutations = Mutation.DeleteCell(cell) :: mutations)
}

object RowMutation {

  /**
   * Construct a HTable mutation for the specified table and row key.
   *
   * @param table The table to mutate a row in.
   * @param key   The key of the row to mutate.
   */
  def apply(table: String, key: ArraySeq[Byte]): RowMutation = RowMutation(table, key, List())
}
