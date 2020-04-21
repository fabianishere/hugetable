package nl.tudelft.htable.storage.hbase

import nl.tudelft.htable.core.{Node, Tablet}
import nl.tudelft.htable.storage.{StorageDriver, TabletDriver}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, RegionInfoBuilder, TableDescriptorBuilder}
import org.apache.hadoop.hbase.regionserver.{HRegion, MemStoreLAB}
import org.apache.hadoop.hbase.wal.WALFactory
import org.apache.hadoop.hbase.{HConstants, TableName}

/**
 * A [StorageDriver] that uses HBase.
 */
class HBaseStorageDriver(val node: Node, val fs: FileSystem) extends StorageDriver {
  import org.apache.hadoop.hbase.regionserver.ChunkCreator

  ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0, 0, null)

  private val conf = new Configuration(fs.getConf)
  private val rootDir = new Path("htable-regions")
  conf.set(HConstants.HBASE_DIR, rootDir.toString)
  // Use DisabledWALProvider since we cannot get other providers to work currently
  conf.set("hbase.wal.provider", "org.apache.hadoop.hbase.wal.DisabledWALProvider")
  private val factory =
    new WALFactory(conf, s"${node.address.getHostName}_${node.address.getPort}_${System.currentTimeMillis}")

  override def openTablet(tablet: Tablet): TabletDriver = createTablet(tablet, open = true)

  override def createTablet(tablet: Tablet): TabletDriver = createTablet(tablet, open = false)

  private def createTablet(tablet: Tablet, open: Boolean): TabletDriver = {
    val tableName = TableName
      .valueOf(tablet.table)
    val tableDescriptor = TableDescriptorBuilder
      .newBuilder(tableName)
      .setColumnFamily(HBaseStorageDriver.columnFamily)
      .build

    val info = RegionInfoBuilder
      .newBuilder(tableName)
      .setRegionId(tablet.id)
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

  override def close(): Unit = {
    factory.close()
  }
}

object HBaseStorageDriver {
  private[hbase] val columnFamily =
    ColumnFamilyDescriptorBuilder
      .newBuilder("hregion".getBytes("UTF-8"))
      .setMaxVersions(5) // TODO Add option for specifying this
      .build()
}
