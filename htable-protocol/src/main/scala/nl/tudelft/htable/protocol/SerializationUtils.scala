package nl.tudelft.htable.protocol

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.net.InetSocketAddress

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString
import nl.tudelft.htable.core
import nl.tudelft.htable.core._
import nl.tudelft.htable.protocol.client.Mutation.{DeleteFromColumn, DeleteFromRow, SetCell}
import nl.tudelft.htable.protocol.client.{MutateRequest, ReadRequest, ReadResponse, RowRange => PBRowRange, Mutation => PBMutation}
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
    query match  {
      case Get(table, key) => ReadRequest(table, rows = ReadRequest.Rows.RowKey(key))
      case Scan(table, range) => ReadRequest(table, rows = ReadRequest.Rows.RowRange(PBRowRange(range.start, range.end)))
    }
  }

  /**
   * Convert the specified [ReadRequest] to a [Query].
   */
  def toQuery(request: ReadRequest): Query = {
    request.rows match {
      case ReadRequest.Rows.RowKey(key) => Get(request.tableName, key)
      case ReadRequest.Rows.RowRange(PBRowRange(start, end, _)) => Scan(request.tableName, RowRange(start, end))
      case ReadRequest.Rows.Empty =>  throw new IllegalArgumentException("Empty query")
    }
  }


  /**
   * Convert the specified [RowMutation] to a [MutateRequest].
   */
  def toMutateRequest(mutation: RowMutation): MutateRequest = {
    val mutations: List[PBMutation] = mutation.mutations.reverse.map {
      case Mutation.AppendCell(cell) =>
        PBMutation.Mutation.SetCell(SetCell(cell.qualifier, cell.value))
      case Mutation.DeleteCell(cell) =>
        PBMutation.Mutation.DeleteFromColumn(DeleteFromColumn(cell.qualifier))
      case Mutation.Delete =>
        PBMutation.Mutation.DeleteFromRow(DeleteFromRow())
    }.map[PBMutation](mut => PBMutation(mut))
    MutateRequest(tableName = mutation.table, rowKey = mutation.key, mutations)
  }

  /**
   * Convert a [MutateRequest] to [RowMutation]
   */
  def toRowMutation(mutateRequest: MutateRequest): RowMutation = {
    val mutations = mutateRequest.mutations.flatMap[Mutation] { mutation =>
      if (mutation.mutation.isSetCell) {
        val cell = mutation.mutation.setCell.head
        Some(Mutation.AppendCell(RowCell(cell.qualifier, 0, cell.value)))
      } else if (mutation.mutation.isDeleteFromColumn) {
        val cell = mutation.mutation.deleteFromColumn.head
        Some(Mutation.DeleteCell(RowCell(cell.qualifier, 0, ByteString())))
      } else if (mutation.mutation.isDeleteFromRow) {
        Some(Mutation.Delete)
      } else {
        None
      }
    }

    RowMutation(mutateRequest.tableName, mutateRequest.rowKey)
      .copy(mutations = mutations.toList)
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
   * Convert a [Cell] to a Protobuf cell.
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
