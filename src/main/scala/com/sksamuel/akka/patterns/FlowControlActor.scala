package com.sksamuel.akka.patterns

import akka.actor.{ActorRef, Actor}
import scala.collection.mutable.ListBuffer

/**
 * Holds up further messages until the previous messages have
 * been acknowledged.
 *
 * @author Stephen Samuel */
class FlowControlActor(target: ActorRef, windowSize: Int = 1) extends Actor {

  val buffer = new ListBuffer[AnyRef]
  val pending = 0

  def receive = {
    case Acknowledged =>
      blocked = false
      if (buffer.size > 0) send(buffer.remove(0))
    case msg: AnyRef =>
      if (blocked) buffer append msg
      else send(msg)
  }

  def send(msg: AnyRef) = {
    target ! msg
    blocked = true
  }
}
