package com.sksamuel.akka.patterns

import akka.actor.{ActorRef, Actor}

/** @author Stephen Samuel */
class EnvelopingActor(target: ActorRef) extends Actor {

  def receive = {
    case msg: AnyRef => target ! new Envelope(msg)
  }
}
