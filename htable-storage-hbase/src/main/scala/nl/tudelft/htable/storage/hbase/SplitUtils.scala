package nl.tudelft.htable.storage.hbase

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{RegionInfo, TableDescriptor}
import org.apache.hadoop.hbase.io.hfile.CacheConfig
import org.apache.hadoop.hbase.regionserver.{HRegion, HRegionFileSystem, HStoreFile}

import scala.jdk.CollectionConverters._

/**
 * Utilities for splitting HBase [HRegion]s.
 */
private[hbase] object SplitUtils {

  /**
   * Split the stores of a [HRegion] into two using the two daughter regions.
   */
  def splitStores(region: HRegion, left: RegionInfo, right: RegionInfo): Unit = {
    val regionFs = region.getRegionFileSystem
    val htd = region.getTableDescriptor
    // Filter. There is a lag cleaning up compacted reference files. They get cleared
    // after a delay in case outstanding Scanners still have references. Because of this,
    // the listing of the Store content may have straggler reference files. Skip these.
    // It should be safe to skip references at this point because we checked above with
    // the region if it thinks it is splittable and if we are here, it thinks it is
    // splitable.
    val files = regionFs.getStoreFiles("hregion").asScala
    val hcd = htd.getColumnFamily("hregion".getBytes("UTF-8"))
    for (storeFileInfo <- files if !storeFileInfo.isReference) {
      splitStore(
        regionFs,
        new HStoreFile(regionFs.getFileSystem,
                       storeFileInfo,
                       regionFs.getFileSystem.getConf,
                       CacheConfig.DISABLED,
                       hcd.getBloomFilterType,
                       true),
        left,
        right
      )
    }
  }

  /**
   * Split a single [HStoreFile] into two separate files.
   */
  private def splitStore(regionFs: HRegionFileSystem,
                         sf: HStoreFile,
                         left: RegionInfo,
                         right: RegionInfo): (Path, Path) = {
    val pathFirst = regionFs.splitStoreFile(left, "hregion", sf, right.getStartKey, false, null)
    val pathSecond = regionFs.splitStoreFile(right, "hregion", sf, right.getStartKey, true, null)

    (pathFirst, pathSecond)
  }
}
