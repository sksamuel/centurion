package com.sksamuel.akka.patterns

import akka.actor.{ActorRef, Actor}
import scala.collection.mutable.ListBuffer

/**
 * Holds up further messages until the previous message has been received.
 *
 * @author Stephen Samuel */
class ReliableActor(target: ActorRef) extends Actor {

  val buffer = new ListBuffer[AnyRef]
  var blocked = false

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
