package nl.tudelft.htable.storage

import java.io.Closeable

import akka.NotUsed
import akka.stream.scaladsl.Source
import nl.tudelft.htable.core.{Query, Row, RowMutation, Tablet}

/**
 * A driver for managing a particular tablet.
 */
trait TabletDriver extends Closeable {

  /**
   * The tablet that this driver is serving.
   */
  val tablet: Tablet

  /**
   * Perform the specified mutation in the tablet.
   */
  def mutate(mutation: RowMutation): Unit

  /**
   * Query the specified data in the tablet.
   */
  def read(query: Query): Source[Row, NotUsed]
}
