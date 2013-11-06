package com.sksamuel.akka.patterns

import akka.actor.{ActorRef, Actor}
import scala.collection.mutable.{Map => MMap, ListBuffer}

/** @author Stephen Samuel */
class Barrier(target: ActorRef, types: Class[_]*) extends Actor {

  val received = MMap.empty[Class[_], Boolean]
  types.foreach(received.put(_, false))

  val buffer = new ListBuffer[Any]
  var released = false

  def receive = {
    case msg =>
      if (released) {
        target ! msg
      } else {
        buffer.append(msg)
        received.put(msg.getClass, true)
        if (received.forall(_._2)) {
          released = true
          buffer.foreach(target !)
          buffer.clear()
        }
      }
  }
}
