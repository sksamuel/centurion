package com.sksamuel.akka.router

import akka.routing.{Destination, RouteeProvider, Route, RouterConfig}
import akka.actor._
import akka.dispatch.Dispatchers

/**
 * An akka router that routes messages to routees that are created for the
 * processing of that single message. After processing that message they
 * are then terminated via a poison pill message.
 *
 * @author Stephen Samuel */
class OneTimeRouter(instances: Int = 5) extends RouterConfig {

  def routerDispatcher: String = Dispatchers.DefaultDispatcherId
  def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.defaultStrategy

  def createRoute(routeeProvider: RouteeProvider): Route = {

    case (sender, message) =>
      List(Destination(sender, {
        val target = routeeProvider.context.actorOf(routeeProvider.routeeProps)
        val routee = routeeProvider.context.actorOf(Props(classOf[OneTimeRoutee], target))
        routeeProvider.registerRoutees(List(routee))
        routee
      }))
  }
}

class OneTimeRoutee(target: ActorRef) extends Actor {
  def receive = {
    case msg =>
      target ! msg
      target ! PoisonPill
  }
}

