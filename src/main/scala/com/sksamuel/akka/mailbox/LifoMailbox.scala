package com.sksamuel.akka.mailbox

import akka.actor.{ActorSystem, ActorRef}
import java.util.concurrent.ConcurrentLinkedDeque
import akka.dispatch.{MessageQueue, MultipleConsumerSemantics, MailboxType, Envelope}
import scala.collection.mutable.ListBuffer

/** @author Stephen Samuel */
class LifoMailbox extends MailboxType {

  val buffer = new ListBuffer[Envelope]

  def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue = new LifoMessageQueue

  class LifoMessageQueue extends ConcurrentLinkedDeque[Envelope] with MessageQueue with MultipleConsumerSemantics {
    final def enqueue(receiver: ActorRef, e: Envelope): Unit = add(e)
    final def dequeue(): Envelope = removeLast()
    def numberOfMessages: Int = size
    def hasMessages: Boolean = !isEmpty
    def cleanUp(owner: ActorRef, deadLetters: MessageQueue): Unit = {
      if (hasMessages) {
        var envelope = dequeue()
        while (envelope ne null) {
          deadLetters.enqueue(owner, envelope)
          envelope = dequeue()
        }
      }
    }
  }
}