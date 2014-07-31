package com.sksamuel.akka.patterns

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestProbe
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FreeSpec, Matchers}

/** @author Stephen Samuel */
class AtomicTest extends FreeSpec with MockitoSugar with Matchers {

  implicit val system = ActorSystem()
  val probe = TestProbe()

  "an AtomicActor" - {
    "should pause between messages" in {

      val counter = new AtomicInteger(0)

      class TestActor extends AtomicActor {

        override def atomicReceive = {
          case msg =>
            counter.incrementAndGet()
        }
      }

      val actor = system.actorOf(Props(new TestActor))

      actor ! "test"
      actor ! "test"
      actor ! "test"

      Thread.sleep(2000)

      counter.intValue shouldBe 1
    }
    "should resume processing messages after Continue is sent" in {

      val counter = new AtomicInteger(0)

      class TestActor extends AtomicActor {

        override def atomicReceive = {
          case msg =>
            counter.incrementAndGet()
            import context.dispatcher
            import scala.concurrent.duration._
            context.system.scheduler.scheduleOnce(1.second) {
              self ! Continue
            }
        }
      }

      val actor = system.actorOf(Props(new TestActor))

      actor ! "test"
      actor ! "test"
      actor ! "test"

      Thread.sleep(5000)

      counter.intValue shouldBe 3
    }
  }
}

