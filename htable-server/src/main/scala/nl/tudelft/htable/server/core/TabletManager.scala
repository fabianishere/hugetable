package nl.tudelft.htable.server.core

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

/**
 * A tablet manager manages the requests of a single tablet.
 */
object TabletManager {

  /**
   * Commands that are accepted by the [TabletManager].
   */
  sealed trait Command

  /**
   * Construct the behavior for the tablet manager.
   */
  def apply(): Behavior[Command] = Behaviors.empty
}
