package com.sksamuel.akka.patterns

import akka.testkit.{TestProbe, TestActorRef}
import scala.concurrent.duration._

class KeepAliveActorTest extends BaseSpec {
  val probe = TestProbe()
  val keepAlive = TestActorRef(new KeepaliveActor(probe.ref, 1 second))


  "A KeepAliveActor" should "send a Heartbeat if it doesn't receive messages" in {

    probe.expectMsg(1.5 seconds, Heartbeat)
  }

  it should "send a Heartbeat after a message" in {
    keepAlive ! "test"
    probe.expectMsg("test")
    probe.expectMsg(1.5 seconds, Heartbeat)
  }
}
