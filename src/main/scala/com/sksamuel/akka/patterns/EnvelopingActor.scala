package com.sksamuel.akka.patterns

import akka.actor.{ActorRef, Actor}

class EnvelopingActor(target: ActorRef, f: (Any => Iterable[(Attribute, Any)]) = x => Nil)
  extends Actor {

  def receive = {
    case msg: Any =>
      val attributes = f(msg)
      val e = attributes.foldLeft(Envelope(msg))((envelope, a) => envelope.withAttribute(a._1, a._2))
      target ! e

    case msg: Envelope(msg, attributes)
  }
}
