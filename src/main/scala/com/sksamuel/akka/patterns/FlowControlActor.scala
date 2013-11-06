package com.sksamuel.akka.patterns

import akka.actor.{ActorRef, Actor}
import scala.collection.mutable

/**
 * Holds up further messages until the previous messages have
 * been acknowledged.
 *
 * @author Stephen Samuel */
class FlowControlActor(target: ActorRef, windowSize: Int = 1) extends Actor {

  val queue = mutable.Queue.empty[Any]
  var pending = 0

  def receive = {
    case Acknowledged =>
      if (pending > 0) pending = pending - 1
      if (queue.size > 0) {
        target ! queue.dequeue()
        pending = pending + 1
      }
    case msg =>
      if (pending == windowSize) {
        queue enqueue msg
      } else {
        pending = pending + 1
        target ! msg
      }
  }
}
