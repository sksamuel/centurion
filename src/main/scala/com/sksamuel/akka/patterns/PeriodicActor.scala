package com.sksamuel.akka.patterns

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import akka.actor.{Cancellable, Actor}

/** @author Stephen Samuel */
trait PeriodicActor extends Actor {

  implicit val executionContext: ExecutionContext
  private var signal: Cancellable = _
  var interval = 500.millis

  override def preStart() = {
    schedule()
    super.preStart()
  }

  abstract override def receive = super.receive andThen {
    case Tick => schedule()
  }

  def schedule() = {
    cancel()
    signal = context.system.scheduler.scheduleOnce(interval, self, Tick)
  }

  override def postStop(): Unit = cancel()
  private def cancel(): Unit = if (signal != null) signal.cancel()
}

case object Tick