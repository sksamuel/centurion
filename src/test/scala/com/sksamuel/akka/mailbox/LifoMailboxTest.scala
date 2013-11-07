package com.sksamuel.akka.mailbox

import org.scalatest.{OneInstancePerTest, FlatSpec}
import org.scalatest.mock.MockitoSugar
import akka.actor.{Props, ActorSystem}
import akka.testkit.TestProbe

/** @author Stephen Samuel */
class LifoMailboxTest extends FlatSpec with MockitoSugar with OneInstancePerTest {

  implicit val system = ActorSystem()
  val probe = TestProbe()
  val actor = system.actorOf(Props(classOf[SleepingActor], probe.ref).withMailbox("lifo-mailbox"))

  "a lifo mailbox" should "process messages in last in first processed semantics" in {
    actor ! "1st"
    actor ! "2nd"
    actor ! "3rd"
    probe.expectMsg("3rd")
    probe.expectMsg("2nd")
    probe.expectMsg("1st")
  }

  it should "handle empty queues" in {
    probe.expectNoMsg()
  }
}
