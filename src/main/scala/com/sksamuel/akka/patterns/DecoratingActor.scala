package com.sksamuel.akka.patterns

import akka.actor.Actor
import scala.collection.mutable.ListBuffer

/** @author Stephen Samuel */
trait DecoratingActor extends Actor {
  var receivers = new ListBuffer[Actor.Receive]
  def receiver(pf: Actor.Receive): Unit = receivers.append(pf)
  final def receive = {
    case msg => receivers.foreach(pf => if (pf.isDefinedAt(msg)) pf(msg))
  }
}
