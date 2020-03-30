package nl.tudelft.htable.protocol

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString
import nl.tudelft.htable.core
import nl.tudelft.htable.core._
import nl.tudelft.htable.protocol.CoreAdapters._
import nl.tudelft.htable.protocol.client.Mutation.{DeleteFromColumn, DeleteFromRow, SetCell}
import nl.tudelft.htable.protocol.client.{MutateRequest, ReadRequest, ReadResponse, Mutation => PBMutation, RowRange => PBRowRange}

import scala.collection.mutable
import scala.language.implicitConversions

/**
 * Conversions between core classes and Protobuf objects.
 */
object ClientAdapters {

  /**
   * Convert the specified [Query] to a [ReadRequest].
   */
  implicit def queryToProtobuf(query: Query): ReadRequest = {
    query match {
      case Get(table, key) => ReadRequest(table, rows = ReadRequest.Rows.RowKey(key))
      case Scan(table, range, reversed) =>
        ReadRequest(table, reversed = reversed, rows = ReadRequest.Rows.RowRange(PBRowRange(range.start, range.end)))
    }
  }

  /**
   * Convert the specified [ReadRequest] to a [Query].
   */
  implicit def queryToCore(request: ReadRequest): Query = {
    request.rows match {
      case ReadRequest.Rows.RowKey(key)                         => Get(request.tableName, key)
      case ReadRequest.Rows.RowRange(PBRowRange(start, end, _)) => Scan(request.tableName, RowRange(start, end))
      case ReadRequest.Rows.Empty                               => throw new IllegalArgumentException("Empty query")
    }
  }

  /**
   * Convert the specified [RowMutation] to a [MutateRequest].
   */
  implicit def mutationToProtobuf(mutation: RowMutation): MutateRequest = {
    val mutations: List[PBMutation] = mutation.mutations.reverse
      .map {
        case Mutation.PutCell(cell) =>
          PBMutation.Mutation.SetCell(SetCell(cell.qualifier, cell.timestamp, cell.value))
        case Mutation.DeleteCell(cell) =>
          PBMutation.Mutation.DeleteFromColumn(DeleteFromColumn(cell.qualifier, cell.timestamp))
        case Mutation.Delete =>
          PBMutation.Mutation.DeleteFromRow(DeleteFromRow())
      }
      .map[PBMutation](mut => PBMutation(mut))
    MutateRequest(tableName = mutation.table, rowKey = mutation.key, mutations)
  }

  /**
   * Convert a [MutateRequest] to [RowMutation]
   */
  implicit def mutationToCore(mutateRequest: MutateRequest): RowMutation = {
    val mutations = mutateRequest.mutations.flatMap[Mutation] { mutation =>
      if (mutation.mutation.isSetCell) {
        val cell = mutation.mutation.setCell.head
        Some(Mutation.PutCell(RowCell(cell.qualifier, cell.timestamp, cell.value)))
      } else if (mutation.mutation.isDeleteFromColumn) {
        val cell = mutation.mutation.deleteFromColumn.head
        Some(Mutation.DeleteCell(RowCell(cell.qualifier, cell.timestamp, ByteString())))
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
  implicit def readToCore(source: Source[ReadResponse, NotUsed]): Source[Row, NotUsed] = {
    source
      .mapConcat(_.cells)
      .statefulMapConcat { () =>
        var currentRowKey = com.google.protobuf.ByteString.EMPTY

        {
          cell: ReadResponse.RowCell =>
            val newRow = cell.rowKey != currentRowKey
            if (newRow)
              currentRowKey = cell.rowKey
            List((cell, newRow))
        }
      }
      .splitWhen(_._2)
      .map(_._1)
      .fold((ByteString.empty, mutable.ListBuffer[RowCell]())) { case ((_, cells), cell) =>
        val rowCell = RowCell(cell.qualifier, cell.timestamp, cell.value)
        cells.addOne(implicitly[RowCell](rowCell))
        (cell.rowKey, cells)
      }
      .map { case (key, cells) => core.Row(key, cells.toSeq) }
      .concatSubstreams
  }

  /**
   * Convert a [Cell] to a Protobuf cell.
   */
  def cellToProtobuf(row: Row, cell: RowCell): ReadResponse.RowCell =
    ReadResponse.RowCell(rowKey = row.key, qualifier = cell.qualifier, timestamp = cell.timestamp, value = cell.value)
}
