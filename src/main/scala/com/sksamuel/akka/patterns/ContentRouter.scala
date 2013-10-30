package com.sksamuel.akka.patterns

import akka.actor.Actor

/** @author Stephen Samuel */
class ContentRouter(_routes: Route*) extends Actor {

  var routes = Map.empty[Class[_], Any => Unit]
  _routes.foreach(add)

  def add(route: Route): Unit = routes = routes + (route.messageType -> route.f)

  def receive = {
    case any: Any => routes.get(any.getClass) match {
      case Some(f) => f(any)
      case None => unhandled(any)
    }
  }
}

case class Route(messageType: Class[_], f: Any => Unit)
