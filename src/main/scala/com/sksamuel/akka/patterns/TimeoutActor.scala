package com.sksamuel.akka.patterns

import akka.actor.PoisonPill

/**
 * Actor that self destructs after a period of inactivity.
 *
 * @author Stephen Samuel */
trait TimeoutActor extends PeriodicActor {

  abstract override def receive = super.receive andThen {
    case Tick => self ! PoisonPill
    case msg => schedule()
  }
}
