package com.sksamuel.akka.patterns

import akka.actor.{ActorRef, Actor}

/**
 * Actor that returns an ackknowledged message for each message it receives.
 *
 * @author Stephen Samuel */
class AcknowledgingActor(target: ActorRef) extends Actor {
  def receive = {
    case msg: AnyRef =>
      target ! msg
      sender ! Ackknowledged
  }
}

case object Ackknowledged
