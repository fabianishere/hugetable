package nl.tudelft.htable.server

import java.util.concurrent.locks.LockSupport

import com.typesafe.scalalogging.Logger
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListenerAdapter}

/**
 * Main implementation of a tablet server as described in the Google BigTable paper.
 *
 * @param client The ZooKeeper client to use for synchronization.
 * @param root The root path in ZooKeeper at which the servers communicate.
 */
class HTableServer(private val client: CuratorFramework, val root: String) extends Runnable {
  /**
   * The logger instance of this class.
   */
  private val log = Logger[HTableServer]

  /**
   * The [LeaderSelector] instance for performing a leader election via ZooKeeper.
   */
  private val leaderSelector = new LeaderSelector(client, s"$root/leader", new LeaderSelectorListenerAdapter {
    override def takeLeadership(client: CuratorFramework): Unit = {
      log.info("I took Leadership!")
      Thread.sleep(10000)
    }
  });

  /**
   * Run the main logic of the server.
   */
  override def run(): Unit = {
    log.info("Starting HugeTable server")

    // Ensure we re-enqueue when the instance relinquishes leadership
    leaderSelector.autoRequeue()

    log.info(s"Determining master server at $root")
    leaderSelector.start()

    try {
      // Park the current thread until interruption
      LockSupport.park()
    } finally {
      leaderSelector.close()
    }
  }
}
