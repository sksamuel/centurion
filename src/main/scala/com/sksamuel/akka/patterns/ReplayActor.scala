package com.sksamuel.akka.patterns

import akka.actor.{ActorRef, Actor}
import scala.collection.mutable.{Set => MSet}

/** @author Stephen Samuel */
class ReplayActor extends Actor {

  var last: Option[Any] = None
  val subscribers = MSet.empty[ActorRef]

  def receive = {
    case Subscribe =>
      subscribers.add(sender)
      last.foreach(sender !)
      self ! SubscriberShuffle
    case Unsubscribe =>
      subscribers.remove(sender)
      self ! SubscriberShuffle
    case msg: Any =>
      last = Some(msg)
      subscribers.foreach(_ ! msg)
  }
}

case object Subscribe
case object Unsubscribe
case object SubscriberShuffle