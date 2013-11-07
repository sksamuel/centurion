package com.sksamuel.akka.mailbox

import org.scalatest.{OneInstancePerTest, FlatSpec}
import org.scalatest.mock.MockitoSugar
import akka.actor.{ActorRef, Actor, Props, ActorSystem}
import akka.testkit.TestProbe

/** @author Stephen Samuel */
class ExpiringMailboxTest extends FlatSpec with MockitoSugar with OneInstancePerTest {

  implicit val system = ActorSystem()
  val probe = TestProbe()
  val actor = system.actorOf(Props(classOf[SleepingActor], probe.ref).withMailbox("expiring-mailbox"))

  "an expiring mailbox" should "not dispatch a message that has expired" in {
    actor ! "test"
    actor ! "expireplease" // 2nd should have expired by the time sleeping actor awakes from 1st message
    probe.expectMsg("test")
    probe.expectNoMsg()
  }

  it should "handle empty queues" in {
    probe.expectNoMsg()
  }
}

class SleepingActor(target: ActorRef) extends Actor {
  def receive = {
    case msg =>
      Thread.sleep(500) // simulate some long running work
      target ! msg
  }
}

