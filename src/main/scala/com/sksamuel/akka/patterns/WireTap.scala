package com.sksamuel.akka.patterns

import akka.actor.{ActorRef, Actor}

/** @author Stephen Samuel */
trait WireTap extends Actor {

  def listener: ActorRef

  abstract override def receive = {
    case msg =>
      super.receive(msg)
      listener ! msg
  }
}
