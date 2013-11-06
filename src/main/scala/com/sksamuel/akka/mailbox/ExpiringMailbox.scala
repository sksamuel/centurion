package com.sksamuel.akka.mailbox

import scala.concurrent.duration._
import akka.dispatch.{QueueBasedMessageQueue, Envelope, MailboxType}
import akka.actor.{ActorSystem, ActorRef}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util

/** @author Stephen Samuel */
class ExpiringMailbox(expiry: FiniteDuration) extends MailboxType {

  def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue = new MessageQueue

  class MessageQueue extends ConcurrentLinkedQueue[ExpiringMessage] with QueueBasedMessageQueue {
    final def queue: util.Queue[ExpiringMessage] = this
    final def enqueue(receiver: ActorRef, e: Envelope): Unit = queue add ExpiringMessage(e)
    final def dequeue(): Envelope = {
      Option(queue.poll()) match {
        case Some(ex) if ex.expire < System.currentTimeMillis() => ex.envelope
        case Some(ex) => dequeue()
        case None => null
      }
    }
  }

  case class ExpiringMessage(envelope: Envelope, expire: Long = System.currentTimeMillis() + expiry.toMillis)
}
