package com.sksamuel.akka.patterns

import akka.actor.{Terminated, ActorRef, Actor}
import scala.collection.mutable

/** @author Stephen Samuel */
class WorkpileMaster extends Actor {

  val queue = mutable.Queue.empty[Any]
  val workers = mutable.Queue.empty[ActorRef]

  def receive = {

    case RegisterWorker(worker) =>
      if (queue.isEmpty) {
        workers enqueue worker
        context.watch(worker)
      } else {
        worker ! Work(queue.dequeue())
      }

    case Terminated(worker) =>
      workers.dequeueFirst(_ == worker)

    case Work(work) =>
      if (workers.isEmpty) {
        queue.enqueue(work)
      } else {
        workers.dequeue() ! Work(work)
      }
  }
}

abstract class WorkpileWorker[W](val master: ActorRef) extends Actor {

  override def preStart() = {
    master ! RegisterWorker(self)
  }

  def receive = {
    case Work(work: W) => process(work)
  }

  def process(work: W)
}

case class RegisterWorker(worker: ActorRef)
case class Work(any: Any)