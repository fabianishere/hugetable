package nl.tudelft.htable.storage.hbase

import java.util

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString
import nl.tudelft.htable.core
import nl.tudelft.htable.core._
import nl.tudelft.htable.storage.TabletDriver
import org.apache.hadoop.hbase.client.{Append, Get, Scan}
import org.apache.hadoop.hbase.regionserver.{HRegion, RegionScanner}
import org.apache.hadoop.hbase.{Cell, CellBuilderFactory, CellBuilderType}

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

    // Force flush for now to prevent data loss
    region.flush(true)
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
        case core.Scan(_, range) =>
          val scan = new Scan()
            .withStartRow(range.start.toArray, true)
            .withStopRow(range.end.toArray, true)
            .addFamily("hregion".getBytes("UTF-8"))
          val scanner = region.getScanner(scan).asInstanceOf[RegionScanner]

          new Iterator[Option[Row]] {
            var more = true

            override def hasNext: Boolean = more

            override def next(): Option[Row] = {
              val cells = new util.ArrayList[Cell]()
              region.startRegionOperation();
              try {
                more = scanner.nextRaw(cells)
              } finally {
                region.closeRegionOperation()
              }
              val scalaCells = cells.asScala
              scalaCells.headOption.map { value =>
                val rowKey = ByteString(value.getRowArray)
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
    val res = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
    res.setFamily("hregion".getBytes("UTF-8"))
    res.setType(cellType)
    res.setRow(row.toArray)
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
