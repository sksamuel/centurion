package com.sksamuel.akka.patterns

import scala.concurrent.duration._
import akka.actor.Actor

/** @author Stephen Samuel */
class PeriodicActor extends Actor {

  override def preStart() = schedule()

  def receive = {
    case Tick => schedule()
  }

  def schedule() = context.system.scheduler.scheduleOnce(500 millis, self, Tick)
}

case object Tick