package com.sksamuel.akka.patterns

import akka.actor.Cancellable
import scala.concurrent.duration._

/**
 * Actor that self destructs after a period of inactivity.
 *
 * @author Stephen Samuel */
trait TimeoutActor extends PatternActor {

  private var timeout: Cancellable = _

  override def preStart(): Unit = {
    scheduleCancel()
  }

  def scheduleCancel(): Unit = timeout = context.system.scheduler.scheduleOnce(30 seconds, self, Timeout)

  def handlers = super.handlers.andThen {
    case Timeout =>
    case _ =>
      if (timeout != null) timeout.cancel()
      scheduleCancel()
  }
}

case object Timeout
