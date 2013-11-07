package com.sksamuel.akka.patterns

import akka.actor.{Actor, ActorRef}

/** @author Stephen Samuel */
class Forwarder(target: ActorRef) extends Actor {
  def receive: Actor.Receive = {
    case msg => target ! msg
  }
}
