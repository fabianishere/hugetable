package nl.tudelft.htable.tests

import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

/**
 * A simple test suite for HugeTable.
 */
class SimpleTest {
  /**
   * Setup the HDFS and ZooKeeper cluster.
   */
  @BeforeEach
  def setUp(): Unit = {
    TestUtils.startHDFS()
    TestUtils.startZooKeeper()
  }

  /**
   * A simple test
   */
  @Test
  def test(): Unit = {

  }

  /**
   * Tear down the HDFS and ZooKeeper cluster.
   */
  @AfterEach
  def tearDown(): Unit = {
    TestUtils.stopHDFS()
    TestUtils.stopZooKeeper()
  }

}
