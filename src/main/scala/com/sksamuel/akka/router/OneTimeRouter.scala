package com.sksamuel.akka.router

/**
 * An akka router that routes messages to routees that are created for the
 * processing of that single message. After processing that message they
 * are then terminated via a poison pill message.
 *
 * @author Stephen Samuel */
//case class OneTimeRouter(instances: Int = 5) extends RouterConfig {
//
//  def routerDispatcher: String = Dispatchers.DefaultDispatcherId
//  def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.defaultStrategy
//
//  def createRoute(routeeProvider: RouteeProvider): Route = {
//
//    case (sender, message) =>
//      List(Destination(sender, {
//        val target = routeeProvider.context.actorOf(routeeProvider.routeeProps)
//        routeeProvider.registerRoutees(List(target))
//        target
//      }))
//  }
//}
