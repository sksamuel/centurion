package com.sksamuel.akka.patterns

import akka.actor.{PoisonPill, Cancellable}
import scala.concurrent.duration._

/**
 * Actor that self destructs after a period of inactivity.
 *
 * @author Stephen Samuel */
trait TimeoutActor extends PatternActor {

  var timeout = 30.seconds
  private var signal: Cancellable = _

  override def preStart() = {
    scheduleCancel()
    super.preStart()
  }

  def scheduleCancel(): Unit = {
    if (signal != null) signal.cancel()
    signal = context.system.scheduler.scheduleOnce(timeout, self, Timeout)
  }

  abstract override def receive = {
    case Timeout => self ! PoisonPill
    case msg =>
      scheduleCancel()
      super.receive(msg)
  }
}

case object Timeout
