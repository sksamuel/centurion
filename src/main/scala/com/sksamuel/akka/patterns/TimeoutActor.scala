package com.sksamuel.akka.patterns

import akka.actor.{Cancellable, Actor}
import scala.concurrent.duration._

/**
 * Actor that self destructs after a period of inactivity.
 *
 * @author Stephen Samuel */
class TimeoutActor extends Actor {

  var timeout: Cancellable = _

  override def preStart(): Unit = {
    scheduleCancel()
  }

  def receive = {
    case Timeout =>
    case _ =>
      timeout.cancel()
      scheduleCancel()
  }

  def scheduleCancel(): Unit = timeout = context.system.scheduler.scheduleOnce(30 seconds, self, Timeout)
}

case object Timeout
