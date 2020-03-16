package nl.tudelft.htable.server.core

import java.util.UUID
import java.util.concurrent.locks.LockSupport

import com.typesafe.scalalogging.Logger
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListenerAdapter}
import org.apache.curator.framework.recipes.nodes.GroupMember

/**
 * Main implementation of a tablet server as described in the Google BigTable paper.
 *
 * @param client The ZooKeeper client to use for synchronization.
 */
class HTableServer(private val client: CuratorFramework) extends Runnable {
  /**
   * The logger instance of this class.
   */
  private val log = Logger[HTableServer]

  /**
   * The [LeaderSelector] instance for performing a leader election via ZooKeeper.
   */
  private val leaderSelector = new LeaderSelector(client, "leader", new LeaderSelectorListenerAdapter {
    override def takeLeadership(client: CuratorFramework): Unit = {
      log.info("I took Leadership!")
      Thread.sleep(10000)
    }
  })

  /**
   * The [GroupMember] instance for keeping track of the tablet servers.
   */
  private val membership = new GroupMember(client, "servers", UUID.randomUUID().toString)

  /**
   * Run the main logic of the server.
   */
  override def run(): Unit = {
    log.info("Starting HugeTable server")

    log.info("Starting leader selection")
    // Ensure we re-enqueue when the instance relinquishes leadership
    leaderSelector.autoRequeue()
    leaderSelector.start()

    log.info("Requesting group membership")
    membership.start()

    try {
      // Park the current thread until interruption
      LockSupport.park()
    } finally {
      leaderSelector.close()
      membership.close()
    }
  }
}
