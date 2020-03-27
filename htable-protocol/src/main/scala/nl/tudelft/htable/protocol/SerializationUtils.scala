package nl.tudelft.htable.protocol

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.net.InetSocketAddress

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString
import nl.tudelft.htable.core
import nl.tudelft.htable.core._
import nl.tudelft.htable.protocol.client.Mutation.{DeleteFromColumn, DeleteFromRow, SetCell}
import nl.tudelft.htable.protocol.client.{MutateRequest, ReadRequest, ReadResponse, RowSet, RowRange => PBRowRange}
import nl.tudelft.htable.protocol.internal.{Tablet => PBTablet}

import scala.language.implicitConversions

/**
 * Utilities for (de)serializing objects between tablet servers.
 */
private[htable] object SerializationUtils {
  /**
   * Serialize an [InetSocketAddress] to a byte string.
   */
  def serialize(value: InetSocketAddress): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close()
    stream.toByteArray
  }

  /**
   * Convert the specified byte string into a socket address.
   */
  def deserialize(bytes: Array[Byte]): InetSocketAddress = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close()
    value.asInstanceOf[InetSocketAddress]
  }

  /**
   * Convert the specified [Query] to a [ReadRequest].
   */
  def toReadRequest(query: Query): ReadRequest = {
    val rowSet = RowSet(
      rowKeys = query.rowKeys.reverse.map(buf => buf),
      rowRanges = query.ranges.reverse.map(range => PBRowRange(startKey = range.start, endKey = range.end))
    )
    ReadRequest(
      tableName = query.table,
      rows = Some(rowSet),
      rowsLimit = query.limit
    )
  }

  /**
   * Convert the specified [ReadRequest] to a [Query].
   */
  def toQuery(request: ReadRequest): Query = {
    val query = Query(request.tableName)
      .limit(request.rowsLimit)

    request.rows match {
      case Some(value) =>
        query.copy(
          rowKeys = value.rowKeys.map(s => protobufToAkka(s)).toList,
          ranges = value.rowRanges.map(range => RowRange(start = range.startKey, end = range.endKey)).toList
        )
      case None => query
    }
  }


  /**
   * Convert the specified [RowMutation] to a [MutateReq
   */
  def toMutateRequest(mutation: RowMutation): MutateRequest = {
    MutateRequest(tableName = mutation.table, rowKey = mutation.key, mutations = mutation.mutations.reverse.map {
      case Mutation.AppendCell(cell) =>
        SetCell(cell.qualifier, cell.value).asInstanceOf[nl.tudelft.htable.protocol.client.Mutation]
      case Mutation.DeleteCell(cell) =>
        DeleteFromColumn(cell.qualifier).asInstanceOf[nl.tudelft.htable.protocol.client.Mutation]
      case Mutation.Delete =>
        DeleteFromRow().asInstanceOf[nl.tudelft.htable.protocol.client.Mutation]
    })
  }

  /**
   * Convert the source of [ReadResponse]s into a source of [Row]s.
   */
  def toRows(source: Source[ReadResponse, NotUsed]): Source[Row, NotUsed] = {
    source.mapConcat(_.cells)
      .sliding(2)
      .splitAfter { slidingElements =>
        // Group cells by their row
        if (slidingElements.size == 2) {
          val current = slidingElements.head
          val next = slidingElements.tail.head
          current.rowKey != next.rowKey
        } else {
          false
        }
      }
      .map { cells =>
        val first = cells.head
        core.Row(first.rowKey, cells.map(cell => RowCell(cell.qualifier, cell.timestamp, cell.value)))
      }
      .mergeSubstreams
  }

  /**
   *
   */
  def toPBCell(row: Row, cell: RowCell): ReadResponse.RowCell = ReadResponse.RowCell(rowKey = row.key, cell.qualifier, cell.timestamp, cell.value)

  /**
   * Translate a core [Tablet] to Protobuf [Tablet].
   */
  implicit def coreToProtobuf(tablet: Tablet): PBTablet =
    PBTablet(tableName = tablet.table, startKey = tablet.startKey, endKey = tablet.endKey)

  /**
   * Translate a core [Tablet] to Protobuf [Tablet].
   */
  implicit def protobufToCore(tablet: PBTablet): Tablet =
    Tablet(tablet.tableName, tablet.startKey, tablet.endKey)

  /**
   * Translate an Akka byte string into a Google Protobuf byte string.
   */
  implicit def akkaToProtobuf(byteString: ByteString): com.google.protobuf.ByteString =
    com.google.protobuf.ByteString.copyFrom(byteString.toByteBuffer)

  /**
   * Translate a Google Protobuf ByteString to Akka ByteString.
   */
  implicit def protobufToAkka(byteString: com.google.protobuf.ByteString): ByteString =
    ByteString(byteString.asReadOnlyByteBuffer())
}
