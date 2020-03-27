package nl.tudelft.htable.storage.hbase

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.apache.hadoop.hbase.client.{Append, Delete, Get, Result, Scan}
import nl.tudelft.htable.core.{Mutation, Query, Row, RowCell, RowMutation, Tablet}
import nl.tudelft.htable.storage.TabletDriver
import org.apache.hadoop.hbase.{Cell, CellBuilderFactory, CellBuilderType}
import org.apache.hadoop.hbase.regionserver.{HRegion, RegionScanner}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

/**
 * An implementation of [TabletDriver] for HBase, corresponding to a single [HRegion].
 */
class HBaseTabletDriver(private val region: HRegion, override val tablet: Tablet) extends TabletDriver {

  /**
   * Perform the specified mutation in the tablet.
   */
  override def mutate(mutation: RowMutation): Unit = {
    val append = new Append(mutation.key.toArray)

    for (cellMutation <- mutation.mutations) {
      append.add(cellMutation match {
        case Mutation.AppendCell(cell) => toHBase(mutation.key, cell, Cell.Type.Put)
        case Mutation.DeleteCell(cell) => toHBase(mutation.key, cell, Cell.Type.DeleteColumn)
        case Mutation.Delete =>
          val res = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
          res.setFamily("hregion".getBytes("UTF-8"))
          res.setType(Cell.Type.Delete)
          res.setRow(mutation.key.toArray)
          res.build()
      })
    }

    region.append(append)
  }

  /**
   * Query the specified data in the tablet.
   */
  override def read(query: Query): Source[Row, NotUsed] = {
    Source.fromIterator[Row] { () =>
      val gets = query.rowKeys
        .map(key => new Get(key.toArray))
        .map(get => region.get(get))
        .map(
          res =>
            Row(
              ByteString(res.getRow),
              res.listCells().asScala.map(fromHBase).toSeq
          ))

      query.ranges
        .map(range => new Scan().withStartRow(range.start.toArray).withStopRow(range.end.toArray))
        .map(scan => region.getScanner(scan).asInstanceOf[RegionScanner])
        .map[Iterator[Row]](scanner =>
          new Iterator[Row] {
            var done = false

            override def hasNext: Boolean = done

            override def next(): Row = {
              val cells = ListBuffer[Cell]()
              done = scanner.nextRaw(cells.asJava)
              val rowKey = ByteString(cells.head.getRowArray)
              val rowCells = cells.map(fromHBase).toSeq
              Row(rowKey, rowCells)
            }
        })
        .foldLeft(gets.iterator)(_ ++ _)
    }
  }

  override def close(): Unit = region.close()

  /**
   * Convert to a HBase cell.
   */
  private def toHBase(row: ByteString, cell: RowCell, cellType: Cell.Type): Cell = {
    val res = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
    res.setFamily("hregion".getBytes("UTF-8"))
    res.setType(cellType)
    res.setRow(row.toArray)
    res.setTimestamp(System.currentTimeMillis())
    res.setQualifier(cell.qualifier.toArray)
    res.setValue(cell.value.toArray)

    res.build()
  }

  /**
   * Convert from a HBase cell.
   */
  private def fromHBase(cell: Cell): RowCell =
    RowCell(ByteString(cell.getQualifierArray), cell.getTimestamp, ByteString(cell.getValueArray))
}
