package com.sksamuel.akka.patterns

import akka.actor.ActorRef
import scala.collection.mutable.ListBuffer

/** @author Stephen Samuel */
class Resequencer(types: Seq[Class[_]], target: ActorRef) extends PatternActor {

  val buffers = types.map(arg => new ListBuffer[AnyRef])

  override def handlers = {
    case msg: AnyRef =>
      types.indexOf(msg.getClass) match {
        case -1 => unhandled(msg)
        case pos: Int =>
          buffers(pos).append(msg)
          checkForCompleteSequence()
      }
  }

  def checkForCompleteSequence(): Unit = {
    if (buffers.forall(_.size > 0)) {
      buffers.foreach(target ! _.remove(0))
    }
  }
}
