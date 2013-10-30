package com.sksamuel.akka.patterns

import akka.actor.{ActorRef, Actor}

/** @author Stephen Samuel */
class MessageFilter(filter: Any => Boolean, target: ActorRef) extends Actor {

  def receive = {
    case any: Any if filter(any) => target ! any
    case any: Any => unhandled(any)
  }
}
