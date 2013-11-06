package com.sksamuel.akka.patterns

import akka.actor.PoisonPill

/**
 * Actor that self destructs after a period of inactivity.
 *
 * @author Stephen Samuel */
trait TimeoutActor extends PeriodicActor {

  receiver {
    case Tick =>
      cancel()
      self ! Timeout
      self ! PoisonPill
    case msg =>
      schedule()
  }
}

case object Timeout
