package com.sksamuel.akka.patterns

import akka.actor.{ActorRef, Actor}
import scala.collection.mutable.ListBuffer

/** @author Stephen Samuel */
class PausableActor(target: ActorRef) extends Actor {

  val buffer = new ListBuffer[Any]
  var paused = false

  def receive: Actor.Receive = {
    case PauseService =>
      paused = true
    case ResumeService =>
      paused = false
      buffer.foreach(target !)
      buffer.clear()
    case msg =>
      if (paused) buffer.append(msg)
      else target ! msg
  }

}

case object PauseService
case object ResumeService
