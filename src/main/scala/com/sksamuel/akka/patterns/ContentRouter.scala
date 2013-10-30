package com.sksamuel.akka.patterns

import akka.actor.{ActorRef, Actor}
import scala.collection.mutable.ListBuffer

/** @author Stephen Samuel */
class ContentRouter extends Actor {

  val routes = new ListBuffer[RouteDefinition]

  def receive = {
    case route: RouteDefinition => routes.append(route)
    case any: Any => routes.find(_).foreach(_.target ! any)
  }
}

case class RouteDefinition(f: Any => Boolean, target: ActorRef)
