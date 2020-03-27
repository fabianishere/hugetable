package nl.tudelft.htable.storage

import java.io.Closeable

import nl.tudelft.htable.core.Tablet

/**
 * A storage driver manages the storage and retrieval of data stored in the HTable database.
 */
trait StorageDriver extends Closeable {

  /**
   * Open the specified tablet.
   */
  def openTablet(tablet: Tablet): TabletDriver

  /**
   * Create a new tablet.
   */
  def createTablet(tablet: Tablet): TabletDriver
}
