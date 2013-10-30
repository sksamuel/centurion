package com.sksamuel.akka.patterns

import akka.actor.Actor
import scala._
import scala.Some

/** @author Stephen Samuel */
class ContentRouter(_routes: Route[_]*) extends Actor {

  var routes = Map.empty[Class[_], Any => Unit]
  _routes.foreach(add)

  def add(route: Route[_]): Unit = routes = routes + (route.messageType -> route.f.asInstanceOf[(Any => Unit)])

  def receive = {
    case any: Any => routes.get(any.getClass) match {
      case Some(f) => f(any)
      case None => unhandled(any)
    }
  }
}

case class Route[T](messageType: Class[T], f: T => Unit)
object Route {
  def apply[T: Manifest](f: T => Unit) = new Route(manifest.runtimeClass.asInstanceOf[Class[T]], f)
}
