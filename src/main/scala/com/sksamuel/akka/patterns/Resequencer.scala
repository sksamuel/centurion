package com.sksamuel.akka.patterns

import akka.actor.{Actor, ActorRef}
import scala.collection.mutable.{Map => MMap}

/** @author Stephen Samuel */
class Resequencer(target: ActorRef, sequenceStart: Int = 1) extends Actor {

  val buffer = MMap.empty[Int, Envelope[_]]
  var expectedSequenceNo = sequenceStart

  def receive = {
    case msg: Envelope[_] =>
      msg.attributes.get(SequenceAttribute) match {
        case Some(seq: Int) if expectedSequenceNo == seq =>
          target ! msg
          expectedSequenceNo += 1
          catchUp()
        case Some(seq: Int) =>
          buffer.put(seq, msg)
        case None =>
          unhandled(msg)
      }
  }

  private def catchUp() {
    while (buffer.contains(expectedSequenceNo)) {
      buffer.remove(expectedSequenceNo) foreach (target !)
    }
  }
}
