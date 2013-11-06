package com.sksamuel.akka.patterns

import akka.actor.{PoisonPill, Cancellable}
import scala.concurrent.duration._

/**
 * Actor that self destructs after a period of inactivity.
 *
 * @author Stephen Samuel */
trait TimeoutActor extends PatternActor {

  private var signal: Cancellable = _

  override def preStart(): Unit = scheduleCancel()

  def scheduleCancel(): Unit = {
    if (signal != null) signal.cancel()
    signal = context.system.scheduler.scheduleOnce(30 seconds, self, Timeout)
  }

  def handlers = super.handlers.andThen {
    case Timeout => self ! PoisonPill
    case _ => scheduleCancel()
  }
}

case object Timeout
