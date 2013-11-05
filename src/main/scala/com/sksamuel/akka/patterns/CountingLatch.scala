package com.sksamuel.akka.patterns

import akka.actor.ActorRef
import scala.collection.mutable.ListBuffer

/**
 * Waits for a specified number of messages before sending those to the target
 * as a single collection of messages.
 *
 * @author Stephen Samuel */
class CountingLatch(count: Int, target: ActorRef) extends PatternActor {

  val received = new ListBuffer[AnyRef]

  def handlers = {
    case msg: AnyRef =>
      received.append(msg)
      if (received.size == count) {
        target ! received.toList
        received.clear()
      }
  }
}
