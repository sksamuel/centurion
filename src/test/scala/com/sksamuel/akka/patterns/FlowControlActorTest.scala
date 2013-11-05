package com.sksamuel.akka.patterns

import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, OneInstancePerTest}
import akka.testkit.{TestProbe, TestActorRef}
import akka.actor.ActorSystem
import scala.concurrent.duration._

/** @author Stephen Samuel */
class FlowControlActorTest extends FlatSpec with MockitoSugar with OneInstancePerTest {

  implicit val system = ActorSystem()
  val probe = TestProbe()
  val actorRef = TestActorRef(new FlowControlActor(probe.ref))

  "a reliable actor" should "block subsequent messages if waiting acknowledgement" in {
    actorRef ! "foo"
    probe.expectMsg("foo")
    actorRef ! "bar"
    probe.expectNoMsg(1 seconds)
    actorRef ! Acknowledged
    probe.expectMsg("bar")
  }

  it should "send subsequent messages immediately if in ready state" in {
    actorRef ! "foo"
    probe.expectMsg("foo")
    actorRef ! Acknowledged
    actorRef ! "bar"
    probe.expectMsg("bar")
  }

  it should "queue received messages while waiting for ackknowlegements" in {
    actorRef ! "foo"
    actorRef ! "bar"
    actorRef ! "baz"
    probe.expectMsg("foo")
    actorRef ! Acknowledged
    probe.expectMsg("bar")
    actorRef ! Acknowledged
    probe.expectMsg("baz")
  }
}
