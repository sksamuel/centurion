package com.sksamuel.akka.patterns

import akka.actor.{Terminated, Actor, ActorRef}
import scala.collection.mutable.ListBuffer

/**
 * Waits for a specified number of messages before sending those to the target
 * as a single collection of messages.
 *
 * @author Stephen Samuel */
class CountdownLatch(count: Int, target: ActorRef) extends Actor {

  val received = new ListBuffer[Any]
  var released = false

  override def receive = {
    case Terminated(targ) => context.stop(self)
    case msg =>
      if (released) {
        target ! msg
      } else {
        received.append(msg)
        if (received.size == count) {
          received.foreach(target !)
          received.clear()
          released = true
        }
      }
  }
}
