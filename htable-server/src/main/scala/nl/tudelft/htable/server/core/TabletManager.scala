package nl.tudelft.htable.server.core

import akka.actor.typed.{Behavior, PostStop}
import akka.actor.typed.scaladsl.Behaviors
import nl.tudelft.htable.core.{Row, Tablet}
import nl.tudelft.htable.storage.StorageDriver

/**
 * A tablet manager manages the requests of a single tablet.
 */
object TabletManager {

  /**
   * Construct the behavior for the tablet manager.
   */
  def apply(storageDriver: StorageDriver, tablet: Tablet): Behavior[NodeManager.Command] = Behaviors.setup { context =>
    context.log.info(s"Opening storage driver for $tablet")
    val driver = storageDriver.openTablet(tablet)

    Behaviors
      .receiveMessagePartial[NodeManager.Command] {
        case NodeManager.Read(query, replyTo) =>
          context.log.info(s"READ $query")
          replyTo ! NodeManager.ReadResponse(driver.read(query))
          Behaviors.same
        case NodeManager.Mutate(mutation, replyTo) =>
          context.log.info(s"MUTATE $mutation")
          driver.mutate(mutation)
          replyTo ! NodeManager.MutateResponse
          Behaviors.same
      }
      .receiveSignal {
        case (_, PostStop) =>
          context.log.info(s"Closing storage driver for $tablet")
          driver.close()
          Behaviors.same
      }
  }
}
