package nl.tudelft.htable.protocol

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString
import nl.tudelft.htable.core
import nl.tudelft.htable.core.{Get, Mutation, Query, Row, RowCell, RowMutation, RowRange, Scan}
import nl.tudelft.htable.protocol.CoreAdapters._
import nl.tudelft.htable.protocol.client.Mutation.{DeleteFromColumn, DeleteFromRow, SetCell}
import nl.tudelft.htable.protocol.client.{MutateRequest, Mutation => PBMutation, ReadRequest, ReadResponse, RowRange => PBRowRange}

import scala.language.implicitConversions

/**
 * Conversions between core classes and Protobuf objects.
 */
object ClientAdapters {
  /**
   * Convert the specified [Query] to a [ReadRequest].
   */
  implicit def queryToProtobuf(query: Query): ReadRequest = {
    query match  {
      case Get(table, key) => ReadRequest(table, rows = ReadRequest.Rows.RowKey(key))
      case Scan(table, range) => ReadRequest(table, rows = ReadRequest.Rows.RowRange(PBRowRange(range.start, range.end)))
    }
  }

  /**
   * Convert the specified [ReadRequest] to a [Query].
   */
  implicit def queryToCore(request: ReadRequest): Query = {
    request.rows match {
      case ReadRequest.Rows.RowKey(key) => Get(request.tableName, key)
      case ReadRequest.Rows.RowRange(PBRowRange(start, end, _)) => Scan(request.tableName, RowRange(start, end))
      case ReadRequest.Rows.Empty =>  throw new IllegalArgumentException("Empty query")
    }
  }

  /**
   * Convert the specified [RowMutation] to a [MutateRequest].
   */
  implicit def mutationToProtobuf(mutation: RowMutation): MutateRequest = {
    val mutations: List[PBMutation] = mutation.mutations.reverse.map {
      case Mutation.AppendCell(cell) =>
        PBMutation.Mutation.SetCell(SetCell(cell.qualifier, cell.timestamp, cell.value))
      case Mutation.DeleteCell(cell) =>
        PBMutation.Mutation.DeleteFromColumn(DeleteFromColumn(cell.qualifier, cell.timestamp))
      case Mutation.Delete =>
        PBMutation.Mutation.DeleteFromRow(DeleteFromRow())
    }.map[PBMutation](mut => PBMutation(mut))
    MutateRequest(tableName = mutation.table, rowKey = mutation.key, mutations)
  }

  /**
   * Convert a [MutateRequest] to [RowMutation]
   */
  implicit def mutationToCore(mutateRequest: MutateRequest): RowMutation = {
    val mutations = mutateRequest.mutations.flatMap[Mutation] { mutation =>
      if (mutation.mutation.isSetCell) {
        val cell = mutation.mutation.setCell.head
        Some(Mutation.AppendCell(RowCell(cell.qualifier, cell.timestamp, cell.value)))
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
  def cellToProtobuf(row: Row, cell: RowCell): ReadResponse.RowCell = ReadResponse.RowCell(rowKey = row.key,
    qualifier = cell.qualifier, timestamp = cell.timestamp, value = cell.value)
}
