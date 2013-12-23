package com.sksamuel.akka.patterns

import akka.actor.{Terminated, ActorRef, Actor}
import scala.collection.mutable

/** @author Stephen Samuel */
class Aggregator(target: ActorRef, types: Class[_]*) extends Actor {

  val buffers = types.map(arg => mutable.Map.empty[String, Any])

  def receive = {
    case Terminated(targ) => context.stop(self)
    case e: Envelope[_] =>
      e.attributes.get(CorrelationId) match {
        case Some(id: String) =>
          types.indexOf(e.msg.getClass) match {
            case -1 => unhandled(e)
            case pos: Int =>
              buffers(pos).put(id, e)
              checkForCompleteMessage(id)
          }
        case None => unhandled(e)
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
