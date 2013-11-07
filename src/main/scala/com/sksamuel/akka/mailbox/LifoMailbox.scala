package com.sksamuel.akka.mailbox

import akka.actor.{ActorSystem, ActorRef}
import java.util.concurrent.ConcurrentLinkedDeque
import akka.dispatch.{MessageQueue, MultipleConsumerSemantics, MailboxType, Envelope}
import scala.collection.mutable.ListBuffer
import com.typesafe.config.Config

/** @author Stephen Samuel */
class LifoMailbox(settings: ActorSystem.Settings, config: Config) extends MailboxType {

  val buffer = new ListBuffer[Envelope]

  def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue = new LifoMessageQueue

  class LifoMessageQueue extends ConcurrentLinkedDeque[Envelope] with MessageQueue with MultipleConsumerSemantics {
    final def enqueue(receiver: ActorRef, e: Envelope): Unit = add(e)
    final def dequeue(): Envelope = if (hasMessages) removeLast() else null
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