package com.sksamuel.akka.patterns

import akka.actor.{Actor, Stash}

/** @author Stephen Samuel */
abstract class AtomicActor extends Actor with Stash {

  case object Continue

  final def receive = {
    case msg =>
      context become inactive
      atomicReceive(msg)
  }

  def atomicReceive: Actor.Receive

  def inactive: Actor.Receive = {
    case Continue =>
      context become receive
      unstashAll()
    case _ =>
      stash()
  }

}


