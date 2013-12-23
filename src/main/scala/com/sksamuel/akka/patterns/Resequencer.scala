package com.sksamuel.akka.patterns

import akka.actor.{Actor, ActorRef}
import scala.collection.mutable.{Map => MMap}

/** @author Stephen Samuel */
class Resequencer(target: ActorRef) extends Actor {

  val buffer = MMap.empty[Int, Envelope[_]]
  var expectedSequenceNo = 1

  def receive = {
    case msg: Envelope[_] =>
      msg.attributes.get(SequenceAttribute) match {
        case Some(seq: Int) if expectedSequenceNo == seq =>
          target ! msg
          expectedSequenceNo += 1
        case Some(seq: Int) =>
          buffer.put(seq, msg)
        case None =>
          unhandled(msg)
      }
  }
}
