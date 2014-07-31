package com.sksamuel.akka.patterns

import akka.actor.{Actor, Stash}

/** @author Stephen Samuel */
trait AtomicActor extends Actor with Stash {

  abstract override def receive = {
    case msg =>
      context become inactive
      super.receive(msg)
  }

  def inactive: Actor.Receive = {
    case Continue =>
      context become super.receive
      unstashAll()
    case _ =>
      stash()
  }
}

case object Continue