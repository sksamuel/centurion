package com.sksamuel.akka.patterns

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorSystem, Props}
import akka.testkit.{TestActorRef, TestProbe}
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._

class TriggerActorTest extends FunSuite with Matchers {

  implicit val system = ActorSystem()
  implicit val timeout = 10.seconds

  test("trigger actor should wait until the trigger function is true before triggering message") {

    val counter = new AtomicInteger(0)
    val probe = TestProbe()

    // don't trigger until we have received 2 messages
    val actorRef = TestActorRef(
      Props(
        TriggerActor(probe.ref, "ok", "error") {
          case _ => counter.incrementAndGet() == 2
        }
      )
    )

    actorRef ! "foo"
    probe.expectNoMsg(3.seconds)
    actorRef ! "bar"
    probe.expectMsg("ok")
  }

  test("trigger actor should send error message if time out hits") {

    val counter = new AtomicInteger(0)
    val probe = TestProbe()

    implicit val timeout = 3.seconds

    // don't trigger until we have received 2 messages
    val actorRef = TestActorRef(
      Props(
        TriggerActor(probe.ref, "ok", "error") {
          case _ => counter.incrementAndGet() == 2
        }
      )
    )

    probe.expectMsg(10.seconds, "error")
  }
}
