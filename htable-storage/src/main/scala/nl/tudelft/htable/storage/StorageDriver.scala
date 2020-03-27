package nl.tudelft.htable.storage

import nl.tudelft.htable.core.Tablet

/**
 * A storage driver manages the storage and retrieval of data stored in the HTable database.
 */
trait StorageDriver {

  /**
   * Open the specified tablet.
   */
  def openTablet(tablet: Tablet): TabletDriver

  /**
   * Create a new tablet.
   */
  def createTablet(tablet: Tablet): TabletDriver
}
