package com.sksamuel.akka.patterns

import akka.actor.{ActorRef, Actor}
import scala.collection.mutable

/** @author Stephen Samuel */
class Aggregator(target: ActorRef, types: Class[_]*) extends Actor {

  val buffers = types.map(arg => mutable.Map.empty[String, Any])

  def receive = {
    case Envelope(msg, id) =>
      types.indexOf(msg.getClass) match {
        case -1 => unhandled(msg)
        case pos: Int =>
          buffers(pos).put(id, msg)
          checkForCompleteMessage(id)
      }
    case msg: Any => unhandled(msg)
  }

  def checkForCompleteMessage(correlationId: String): Unit = {
    if (buffers.forall(_.contains(correlationId))) {
      val msg = buffers.flatMap(_.remove(correlationId))
      target ! msg
    }
  }
}
