package com.sksamuel.akka.patterns

/**
 * The AcknowledgingActor will send an ack to the sender as soon as a message is received
 * before continuing with processing the message.
 * Uses the stackable trait pattern with DecoratingActor. Override receiver in traits that wish to use this actor.
 * See DecoratingActor.
 * This actor is most often used as the other end to the flow control actors.
 *
 * @author Stephen Samuel */
trait AcknowledgingActor extends DecoratingActor {
  receiver {
    case msg: AnyRef => sender ! Acknowledged
  }
}

case object Acknowledged
