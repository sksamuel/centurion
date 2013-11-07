package com.sksamuel.akka.patterns

import akka.actor.ActorRef

/**
 * Actor that sends a heartbeat message if no other messages have been received within
 * a user defined duration.
 *
 * @author Stephen Samuel */
class KeepaliveActor(target: ActorRef) extends PeriodicActor {

  receiver {
    case Tick => target forward Heartbeat
    case msg =>
      target forward msg
      schedule()
  }
}

case object Heartbeat
