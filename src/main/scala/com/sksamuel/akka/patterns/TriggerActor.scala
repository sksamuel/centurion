package com.sksamuel.akka.patterns

import akka.actor.{Actor, ActorRef}

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
class TriggerActor(onSuccess: => Unit,
                   onFailure: => Unit,
                   trigger: PartialFunction[Any, Boolean])
                  (implicit val timeout: FiniteDuration) extends Actor {

  case object Timeout

  import context.dispatcher

  override def preStart(): Unit = {
    context.system.scheduler.scheduleOnce(timeout) {
      self ! Timeout
    }
  }

  override def receive: Receive = {
    case Timeout =>
      onFailure
      context.stop(self)
    case any =>
      if (trigger(any)) {
        onSuccess
        context.stop(self)
      }
  }
}

object TriggerActor {

  /**
    * Overload of trigger actor that will send a success message, or a failure message, to a target
    * when the trigger has completed, or has timed out.
    *
    * @param target  the target of the messages
    * @param success the success message to send
    * @param failure the failure message to send
    * @param trigger the trigger partial function
    * @param timeout execution timeout
    */
  def apply(target: ActorRef, success: Any, failure: Any)(trigger: PartialFunction[Any, Boolean])
           (implicit timeout: FiniteDuration): TriggerActor = new TriggerActor(target ! success, target ! failure, trigger)
}