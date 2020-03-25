package nl.tudelft.htable.client

import akka.util.ByteString

/**
 * A builder class for constructing a HTable query.
 *
 * @param table The name of the table to query.
 * @param limit The maximum number of results that may be returned from the query.
 */
final case class Query private (table: String, limit: Long, rowKeys: List[ByteString]) {

  /**
   * Limit the maximum number of results returned by the query. When the limit is zero, all results will be returned.
   */
  def limit(limit: Long): Query = {
    require(limit >= 0, () => "Limit cannot be negative")
    copy(limit = limit)
  }

  /**
   * Specify a row key to look up.
   */
  def withRow(key: ByteString): Query = copy(rowKeys = key :: rowKeys)
}

object Query {

  /**
   * Construct a HTable query for the specified table.
   *
   * @param table The table to query.
   */
  def apply(table: String): Query = Query(table, 0, List())
}
