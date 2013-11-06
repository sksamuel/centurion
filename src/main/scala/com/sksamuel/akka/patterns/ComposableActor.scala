package com.sksamuel.akka.patterns

import akka.actor.Actor

/** @author Stephen Samuel */
trait ComposableActor extends Actor {
  var receivers: Actor.Receive = Actor.emptyBehavior
  def receiver(next: Actor.Receive): Unit = receivers = receivers andThen next
  final def receive = receivers
}
