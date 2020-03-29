package nl.tudelft.htable.protocol

import nl.tudelft.htable.core.{RowRange, Tablet}
import nl.tudelft.htable.protocol.CoreAdapters._
import nl.tudelft.htable.protocol.internal.{Tablet => PBTablet}

import scala.language.implicitConversions

/**
 * Adapters for internal communication protocol
 */
object InternalAdapters {
  /**
   * Translate a core [Tablet] to Protobuf [Tablet].
   */
  implicit def tabletToProtobuf(tablet: Tablet): PBTablet =
    PBTablet(tableName = tablet.table, startKey = tablet.range.start, endKey = tablet.range.end)

  /**
   * Translate a core [Tablet] to Protobuf [Tablet].
   */
  implicit def tabletToCore(tablet: PBTablet): Tablet =
    Tablet(tablet.tableName, RowRange(tablet.startKey, tablet.endKey))
}
