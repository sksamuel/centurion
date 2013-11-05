package com.sksamuel.akka.patterns

import akka.actor.Actor

/** @author Stephen Samuel */
trait PatternActor extends Actor {
  def handlers: PartialFunction[Any, Unit]
  def receive: Actor.Receive = handlers
}
