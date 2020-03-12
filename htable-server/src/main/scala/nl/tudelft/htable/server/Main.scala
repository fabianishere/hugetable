package nl.tudelft.htable.server

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry

/**
 * Main class of the HugeTable server program.
 */
object Main {

  /**
   * Main entry point of the program.
   *
   * @param args The command line arguments passed to the program.
   */
  def main(args: Array[String]): Unit = {
    val zookeeper = CuratorFrameworkFactory.newClient("localhost:2181", new ExponentialBackoffRetry(1000, 3))
    zookeeper.start()
    zookeeper.blockUntilConnected()

    val server = new HTableServer(zookeeper, "/htable")
    server.run()
  }
}
