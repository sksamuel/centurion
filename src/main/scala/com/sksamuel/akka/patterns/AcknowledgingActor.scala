package com.sksamuel.akka.patterns

import akka.actor.Actor

/**
 * The AcknowledgingActor will send an ack to the sender as soon as a message is received
 * before continuing with processing the message.
 * Uses the stackable trait pattern.
 * This actor is most often used as the other end to the flow control actors.
 *
 * @author Stephen Samuel */
trait AcknowledgingActor extends Actor {
  def behavior: PartialFunction[Any, Unit]
  def receive = {
    case msg: AnyRef =>
      sender ! Acknowledged
      behavior.apply(msg)
  }
}

case object Acknowledged
