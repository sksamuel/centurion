package com.sksamuel.akka.patterns

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{TestActor, TestProbe}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FreeSpec, Matchers}

/** @author Stephen Samuel */
class AtomicTest extends FreeSpec with MockitoSugar with Matchers {

  implicit val system = ActorSystem()
  val probe = TestProbe()

  "an AtomicActor" - {
    "should pause between messages" in {

      val counter = new AtomicInteger(0)

      class TestActor extends Actor {

        override def receive = {
          case msg =>
            counter.incrementAndGet()
        }
      }

      class AtomicActor extends TestActor with Atomic
      val actor = system.actorOf(Props(new AtomicActor))

      actor ! "test"
      actor ! "test"
      actor ! "test"

      Thread.sleep(2000)

      counter.intValue shouldBe 1
    }
  }

}

