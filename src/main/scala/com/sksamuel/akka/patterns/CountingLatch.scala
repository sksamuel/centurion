package com.sksamuel.akka.patterns

import akka.actor.{Actor, ActorRef}
import scala.collection.mutable.ListBuffer

/**
 * Waits for a specified number of messages before sending those to the target
 * as a single collection of messages.
 *
 * @author Stephen Samuel */
class CountingLatch(count: Int, target: ActorRef) extends Actor {

  val received = new ListBuffer[AnyRef]

  override def receive = {
    case msg: AnyRef =>
      received.append(msg)
      if (received.size == count) {
        target ! received.toList
        received.clear()
      }
  }
}
