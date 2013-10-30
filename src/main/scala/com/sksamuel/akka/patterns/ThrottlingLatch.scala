package com.sksamuel.akka.patterns

import scala.concurrent.duration.FiniteDuration
import akka.actor.{ActorRef, Actor}

/**
 * Throttles messages such that there is a minimum delay between messages.
 *
 * Let a delay be d. Then when a message is received, if there has been another message within d, then
 * the message is held until d time has elapsed from the previous message.
 *
 * @author Stephen Samuel */
class ThrottlingLatch(duration: FiniteDuration, target: ActorRef) extends Actor {

  var pending: Option[AnyRef] = None
  var throttled = false

  def receive = {
    case ReleaseThrottle =>
      throttled = false
      pending = None
      pending.foreach(target ! _)
    case msg: AnyRef =>
      throttled match {
        case true =>
          // just update the pending msg, the same scheduled timeout is fine
          pending = Some(msg)
        case false =>
          target ! msg
          throttled = true
          context.system.scheduler.scheduleOnce(duration) {
            target ! ReleaseThrottle
          }
      }
  }

  case object ReleaseThrottle
}
