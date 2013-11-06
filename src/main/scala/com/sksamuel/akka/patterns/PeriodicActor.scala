package com.sksamuel.akka.patterns

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import akka.actor.Cancellable

/** @author Stephen Samuel */
trait PeriodicActor extends DecoratingActor {

  implicit val executor: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
  private var signal: Cancellable = _
  var tickGenerator: IntervalGenerator = new FixedIntervalGenerator(500.millis)

  abstract override def preStart() = {
    schedule()
    super.preStart()
  }
  override def postStop(): Unit = cancel()

  receiver {
    case Tick => schedule()
  }

  protected def schedule() {
    cancel()
    signal = context.system.scheduler.scheduleOnce(tickGenerator.duration, self, Tick)
  }

  protected def cancel(): Unit = if (signal != null) signal.cancel()
}

case object Tick

trait IntervalGenerator {
  def duration: FiniteDuration
}
class FixedIntervalGenerator(val duration: FiniteDuration) extends IntervalGenerator