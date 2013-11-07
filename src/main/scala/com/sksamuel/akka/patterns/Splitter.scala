package com.sksamuel.akka.patterns

import akka.actor.{Terminated, ActorRef, Actor}
import scala.collection.JavaConverters._

/**
 * Splits up collection based messages and forwards singularly.
 *
 * @author Stephen Samuel */
class Splitter(target: ActorRef) extends Actor {

  def receive = {
    case Terminated(targ) => context.stop(self)
    case iter: Iterable[_] => iter.foreach(target !)
    case iter: Iterator[_] => iter.foreach(target !)
    case iter: Array[_] => iter.foreach(target !)
    case iter: java.util.Iterator[_] => iter.asScala.foreach(target !)
    case iter: java.lang.Iterable[_] => iter.asScala.foreach(target !)
    case other: AnyRef => target ! other
  }
}
