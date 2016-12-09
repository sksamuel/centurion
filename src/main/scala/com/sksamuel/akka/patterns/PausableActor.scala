package com.sksamuel.akka.patterns

import akka.actor.{ActorRef, Actor, Stash}
import scala.collection.mutable.ListBuffer

/** @author Stephen Samuel */
class PausableActor(target: ActorRef) extends Actor with Stash{
  var paused = false

  def receive: Actor.Receive = {
    case PauseService =>
      paused = true
    case ResumeService =>
      paused = false
      unstashAll()
    case msg =>
      if (paused) stash()
      else target ! msg
  }

}

case object PauseService
case object ResumeService
