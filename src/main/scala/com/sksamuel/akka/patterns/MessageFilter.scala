package com.sksamuel.akka.patterns

import akka.actor.{Terminated, ActorRef, Actor}

/** @author Stephen Samuel */
class MessageFilter(filter: Any => Boolean, target: ActorRef) extends Actor {

  def receive = {
    case Terminated(targ) => context.stop(self)
    case any: Any if filter(any) => target ! any
    case any: Any => unhandled(any)
  }
}
