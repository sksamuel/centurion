package com.sksamuel.akka.patterns

import akka.testkit.{TestProbe, TestActorRef}
import scala.concurrent.duration._

class PausableActorTest extends BaseSpec {
  val probe = TestProbe()
  val pausable = TestActorRef(new PausableActor(probe.ref))


  "A PausableActor" should "not send message when paused" in {
    pausable ! PauseService
    pausable ! "test"
    pausable ! "hello"
    probe.expectNoMsg()
  }

  it should "send message when resumed" in {
    pausable ! PauseService
    pausable ! "test"
    pausable ! "hello"
    probe.expectNoMsg()
    pausable ! ResumeService
    probe.expectMsg("test")
    probe.expectMsg("hello")
  }
}
