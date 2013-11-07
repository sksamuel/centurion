package com.sksamuel.akka.mailbox

import akka.dispatch.{MessageQueue, MailboxType}
import akka.actor.{ActorSystem, ActorRef}
import com.sksamuel.akka.patterns.{Envelope, PriorityAttribute}
import java.util.{Comparator, PriorityQueue}
import akka.dispatch
import com.typesafe.config.Config

/** @author Stephen Samuel */
class PriorityMailbox(settings: ActorSystem.Settings, config: Config) extends MailboxType {

  def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue = new PriorityMessageQueue

  class PriorityMessageQueue
    extends PriorityQueue[dispatch.Envelope](11, new EnvelopePriorityComparator)
    with MessageQueue {

    def cleanUp(owner: ActorRef, deadLetters: MessageQueue): Unit = {
      if (hasMessages) {
        var envelope = dequeue()
        while (envelope ne null) {
          deadLetters.enqueue(owner, envelope)
          envelope = dequeue()
        }
      }
    }
    def hasMessages: Boolean = size > 0
    def numberOfMessages: Int = size
    def dequeue(): dispatch.Envelope = poll()
    def enqueue(receiver: ActorRef, e: dispatch.Envelope): Unit = add(e)
  }

  class EnvelopePriorityComparator extends Comparator[dispatch.Envelope] {
    def compare(o1: dispatch.Envelope, o2: dispatch.Envelope): Int = {
      val priority1 = o1.message.asInstanceOf[Envelope[_]].attributes(PriorityAttribute).toString.toInt
      val priority2 = o2.message.asInstanceOf[Envelope[_]].attributes(PriorityAttribute).toString.toInt
      priority1 compareTo priority2
    }
  }

}
