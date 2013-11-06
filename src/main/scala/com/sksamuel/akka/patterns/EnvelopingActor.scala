package com.sksamuel.akka.patterns

import akka.actor.{ActorRef, Actor}

/** @author Stephen Samuel */
class EnvelopingActor(target: ActorRef, f: (Any => Iterable[(Attribute, Any)]) = x => Nil) extends Actor {
  def receive = {
    case msg: Any =>
      val attributes = f(msg)
      val e = attributes.foldLeft(new Envelope(msg))((b, a) => b.withAttribute(a._1, a._2))
      target ! e
  }
}
