package com.sksamuel.akka.patterns

import akka.actor.{ActorRef, Actor}
import scala.collection.mutable.ListBuffer

/**
 * Waits for a specified number of messages before sending those to the target.
 *
 * @author Stephen Samuel */
class CountingLatch(count: Int, target: ActorRef) extends Actor {

  val received = new ListBuffer[AnyRef]

  def receive = {
    case msg: AnyRef =>
      received.append(msg)
      if (received.size == count) {
        target ! received.toList
        received.clear()
      }
  }
}
