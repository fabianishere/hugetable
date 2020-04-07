package nl.tudelft.htable.storage.hbase

import java.util

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString
import nl.tudelft.htable.core
import nl.tudelft.htable.core._
import nl.tudelft.htable.storage.TabletDriver
import org.apache.hadoop.hbase.client.{Get, Put, RegionInfoBuilder, Scan}
import org.apache.hadoop.hbase.regionserver.{HRegion, Region, RegionScanner}
import org.apache.hadoop.hbase.{Cell, CellBuilderFactory, CellBuilderType, CellUtil}

import scala.jdk.CollectionConverters._

/**
 * An implementation of [TabletDriver] for HBase, corresponding to a single [HRegion].
 */
class HBaseTabletDriver(private val region: HRegion, override val tablet: Tablet) extends TabletDriver {

  var mutations = 0

  /**
   * Perform the specified mutation in the tablet.
   */
  override def mutate(mutation: RowMutation): Unit = {
    val put = new Put(mutation.key.toArray)

    for (cellMutation <- mutation.mutations) {
      put.add(cellMutation match {
        case Mutation.PutCell(cell)    => toHBase(mutation.key, cell, Cell.Type.Put)
        case Mutation.DeleteCell(cell) => toHBase(mutation.key, cell, Cell.Type.DeleteColumn)
        case Mutation.Delete =>
          val res = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
          res.setFamily("hregion".getBytes("UTF-8"))
          res.setType(Cell.Type.Delete)
          res.setRow(mutation.key.toArray)
          res.build()
      })
    }

    region.startRegionOperation(Region.Operation.PUT)
    region.put(put)
    region.flush(false)
    region.closeRegionOperation(Region.Operation.PUT)
  }

  /**
   * Query the specified data in the tablet.
   */
  override def read(query: Query): Source[Row, NotUsed] = {
    Source.fromIterator[Row] { () =>
      query match {
        case core.Get(_, key) =>
          val get = new Get(key.toArray)
          get.setCacheBlocks(true) // Enable caching
          get.addFamily("hregion".getBytes("UTF-8"))
          get.readAllVersions()
          region.startRegionOperation(Region.Operation.GET)
          val result = region.get(get)
          region.closeRegionOperation(Region.Operation.GET)
          if (result.isEmpty)
            Iterator()
          else
            Iterator(Row(ByteString(result.getRow), result.listCells().asScala.map(fromHBase).toSeq))
        case core.Scan(_, range, reversed) =>
          region.startRegionOperation(Region.Operation.SCAN)
          // Note that the start/end row are also reversed when we scan in reverse order due
          // to HBase behavior
          val startRow = if (reversed) range.end.toArray else range.start.toArray
          val endRow = if (reversed) range.start.toArray else range.end.toArray
          val scan = new Scan()
            .setCacheBlocks(true) // Enable caching
            .withStartRow(startRow, !reversed)
            .withStopRow(endRow, reversed)
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

              if (!more) {
                region.closeRegionOperation(Region.Operation.SCAN)
              }

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

  override def split(splitKey: ByteString): (Tablet, Tablet) = {
    region.flush(true)

    val leftTablet = Tablet(tablet.table, RowRange(tablet.range.start, splitKey))
    val leftDaughter = RegionInfoBuilder
      .newBuilder(region.getTableDescriptor.getTableName)
      .setStartKey(tablet.range.start.toArray)
      .setEndKey(splitKey.toArray)
      .setSplit(false)
      .setRegionId(region.getRegionInfo.getRegionId + 1)
      .build

    val rightTablet = Tablet(tablet.table, RowRange(splitKey, tablet.range.end))
    val rightDaughter = RegionInfoBuilder
      .newBuilder(region.getTableDescriptor.getTableName)
      .setStartKey(splitKey.toArray)
      .setEndKey(tablet.range.end.toArray)
      .setSplit(false)
      .setRegionId(region.getRegionInfo.getRegionId + 1)
      .build

    region.startRegionOperation(Region.Operation.SPLIT_REGION)
    val regionFs = region.getRegionFileSystem
    regionFs.createSplitsDir(leftDaughter, rightDaughter)
    SplitUtils.splitStores(region, leftDaughter, rightDaughter)
    region.closeRegionOperation(Region.Operation.SPLIT_REGION)

    (leftTablet, rightTablet)
  }

  override def close(): Unit = {
    // Force flush for now to not lose changes when terminating the process
    region.flush(true)
    region.close()
  }

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
