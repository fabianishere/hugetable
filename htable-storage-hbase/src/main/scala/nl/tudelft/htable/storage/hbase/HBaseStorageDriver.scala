package nl.tudelft.htable.storage.hbase

import nl.tudelft.htable.core.Tablet
import nl.tudelft.htable.storage.{StorageDriver, TabletDriver}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, RegionInfoBuilder, TableDescriptorBuilder}
import org.apache.hadoop.hbase.regionserver.{HRegion, MemStoreLAB}
import org.apache.hadoop.hbase.wal.WALFactory
import org.apache.hadoop.hbase.{HConstants, TableName}

/**
 * A [StorageDriver] that uses HBase.
 */
class HBaseStorageDriver(val fs: FileSystem) extends StorageDriver {
  import org.apache.hadoop.hbase.regionserver.ChunkCreator

  ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0, 0, null)

  override def openTablet(tablet: Tablet): TabletDriver = createTablet(tablet, open = true)

  override def createTablet(tablet: Tablet): TabletDriver = createTablet(tablet, open = false)

  private def createTablet(tablet: Tablet, open: Boolean): TabletDriver = {
    val tableName = TableName.valueOf(tablet.table)
    val tableDescriptor = TableDescriptorBuilder
      .newBuilder(tableName)
      .setColumnFamily(HBaseStorageDriver.columnFamily)
      .build

    val conf = fs.getConf
    val rootDir = new Path("hregions")
    conf.set(HConstants.HBASE_DIR, rootDir.toString)
    conf.set("hbase.wal.provider", "org.apache.hadoop.hbase.wal.DisabledWALProvider")

    val factory = new WALFactory(conf, "hregion-tablet")

    // @todo add region id to be able to reopen files in a later stage
    val info = RegionInfoBuilder
      .newBuilder(tableName)
      .setRegionId(0)
      .setReplicaId(0)
      .setStartKey(tablet.range.start.toArray)
      .setEndKey(tablet.range.end.toArray)
      .setSplit(false)
      .setOffline(false)
      .build

    val WAL = factory.getWAL(info)
    val region =
      if (open)
        HRegion.openHRegion(conf, FileSystem.get(conf), rootDir, info, tableDescriptor, WAL)
      else
        HRegion.createHRegion(info, rootDir, conf, tableDescriptor, WAL, true)
    new HBaseTabletDriver(region, tablet)
  }

  override def close(): Unit = {}
}

object HBaseStorageDriver {
  private val columnFamily =
    ColumnFamilyDescriptorBuilder
      .newBuilder("hregion".getBytes("UTF-8"))
      .setMaxVersions(10) // TODO Add option for specifying this
      .build()
}
