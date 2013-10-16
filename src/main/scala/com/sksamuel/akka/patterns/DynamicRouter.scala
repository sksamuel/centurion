package com.sksamuel.akka.patterns

import akka.actor.{ActorRef, Actor}

/** @author Stephen Samuel */
class DynamicRouter extends Actor {

  private var routes = Map.empty[Class[_], ActorRef]

  def receive = {
    case m: Register =>
      routes = routes + (m.clazz -> sender)
      sender ! RegistrationReceipt(m.clazz)
    case m: Deregister =>
      routes = routes - m.clazz
      sender ! DeregistrationReceipt(m.clazz)
    case EnumerateRoutes =>
      sender ! DynamicRoutes(routes)
  }
}

case class Register(clazz: Class[_])
case class Deregister(clazz: Class[_])
case class RegistrationReceipt(clazz: Class[_])
case class DeregistrationReceipt(clazz: Class[_])
case object EnumerateRoutes
case class DynamicRoutes(routes: Map[Class[_], ActorRef])
