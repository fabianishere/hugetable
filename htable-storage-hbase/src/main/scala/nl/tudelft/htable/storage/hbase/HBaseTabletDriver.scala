package nl.tudelft.htable.storage.hbase

import java.util

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString
import nl.tudelft.htable.core
import nl.tudelft.htable.core._
import nl.tudelft.htable.storage.TabletDriver
import org.apache.hadoop.hbase.client.{Append, Get, Put, Result, Scan}
import org.apache.hadoop.hbase.regionserver.{HRegion, RegionScanner}
import org.apache.hadoop.hbase.{Cell, CellBuilderFactory, CellBuilderType, CellUtil}

import scala.jdk.CollectionConverters._

/**
 * An implementation of [TabletDriver] for HBase, corresponding to a single [HRegion].
 */
class HBaseTabletDriver(private val region: HRegion, override val tablet: Tablet) extends TabletDriver {

  /**
   * Perform the specified mutation in the tablet.
   */
  override def mutate(mutation: RowMutation): Unit = {
    val put = new Put(mutation.key.toArray)

    for (cellMutation <- mutation.mutations) {
      put.add(cellMutation match {
        case Mutation.PutCell(cell) => toHBase(mutation.key, cell, Cell.Type.Put)
        case Mutation.DeleteCell(cell) => toHBase(mutation.key, cell, Cell.Type.DeleteColumn)
        case Mutation.Delete =>
          val res = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
          res.setFamily("hregion".getBytes("UTF-8"))
          res.setType(Cell.Type.Delete)
          res.setRow(mutation.key.toArray)
          res.build()
      })
    }

    region.put(put)
  }

  /**
   * Query the specified data in the tablet.
   */
  override def read(query: Query): Source[Row, NotUsed] = {
    Source.fromIterator[Row] { () =>
      query match {
        case core.Get(_, key) =>
          val result = region.get(new Get(key.toArray))
          if (result.getExists)
            Iterator(Row(ByteString(result.getRow), result.listCells().asScala.map(fromHBase).toSeq))
          else
            Iterator()
        case core.Scan(_, range, reversed) =>
          val scan = new Scan()
            .withStartRow(range.start.toArray, true)
            .withStopRow(range.end.toArray, false)
            .setReversed(reversed)
            .addFamily("hregion".getBytes("UTF-8"))
            .readAllVersions()

          val scanner = region.getScanner(scan).asInstanceOf[RegionScanner]

          new Iterator[Option[Row]] {
            var more = true
            val cells = new util.ArrayList[Cell]()
            override def hasNext: Boolean = more

            override def next(): Option[Row] = {
              cells.clear()
              more = scanner.nextRaw(cells)
              val scalaCells = cells.asScala
              scalaCells.headOption.map { value =>
                val rowKey = ByteString(CellUtil.cloneRow(value))
                val rowCells = scalaCells.map(fromHBase).toSeq
                Row(rowKey, rowCells)
              }
            }
          }.flatten
      }
    }
  }

  override def close(): Unit = region.close()

  /**
   * Convert to a HBase cell.
   */
  private def toHBase(row: ByteString, cell: RowCell, cellType: Cell.Type): Cell = {
    val res = CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
    res.setFamily("hregion".getBytes("UTF-8"))
    res.setType(cellType)
    res.setRow(row.toArray)
    res.setQualifier(cell.qualifier.toArray)
    res.setValue(cell.value.toArray)
    res.setTimestamp(cell.timestamp)
    res.build()
  }

  /**
   * Convert from a HBase cell.
   */
  private def fromHBase(cell: Cell): RowCell =
    RowCell(ByteString(CellUtil.cloneQualifier(cell)), cell.getTimestamp, ByteString(CellUtil.cloneValue(cell)))
}
