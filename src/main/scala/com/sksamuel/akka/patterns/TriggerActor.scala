package com.sksamuel.akka.patterns

import akka.actor.Actor

import scala.concurrent.duration.FiniteDuration

/**
  * Waits for a set of messages to arrive before triggering a function.
  *
  * @param trigger   a function to determine if all the messages have been received. If the partial
  *                  function returns true then the onSuccess function will be triggered.
  * @param onSuccess the function to execute once all messages have been received
  * @param onFailure the function to execute if the timeout is received
  * @param timeout   how long to wait for the trigger messages
  */
class TriggerActor(trigger: PartialFunction[Any, Boolean],
                   onSuccess: () => (),
                   onFailure: () => ())
                  (implicit val timeout: FiniteDuration) extends Actor {

  override def preStart(): Unit = {
    context.system.scheduler.scheduleOnce(timeout) {
      self ! "timeout"
    }
  }

  override def receive: Receive = {
    case "timeout" =>
      onFailure()
      context.stop(self)
    case any =>
      if (trigger(any)) {
        onSuccess()
        context.stop(self)
      }
  }
}
