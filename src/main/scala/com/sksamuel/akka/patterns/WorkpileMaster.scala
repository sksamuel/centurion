package com.sksamuel.akka.patterns

import akka.actor.{Terminated, ActorRef, Actor}
import scala.collection.mutable

/** @author Stephen Samuel */
class WorkpileMaster extends Actor {

  val queue = mutable.Queue.empty[Any]
  val workers = mutable.Queue.empty[ActorRef]

  def receive = {

    case WorkerReady(worker) =>
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

abstract class WorkpileWorker(val master: ActorRef) extends Actor {

  override def preStart() = {
    ready()
  }

  def receive = {
    case Work(work) =>
      process(work)
      ready()
  }

  def ready() = master ! WorkerReady(self)
  def process(work: Any)
}

case class WorkerReady(worker: ActorRef)
case class Work(any: Any)