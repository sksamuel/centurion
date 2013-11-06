package com.sksamuel.akka.patterns

import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, OneInstancePerTest}
import akka.testkit.{TestProbe, TestActorRef}
import akka.actor.{ActorRef, ActorSystem}
import scala.concurrent.duration._

/** @author Stephen Samuel */
class TimeoutActorTest extends FlatSpec with MockitoSugar with OneInstancePerTest {

  implicit val system = ActorSystem()
  val probe = TestProbe()

  "a timeout actor" should "terminate after no msg in the timeout period" in {
    TestActorRef(new TestTimeoutActor(new FixedIntervalGenerator(250 millis), probe.ref))
    probe.expectMsg(Timeout)
  }
}

class TestTimeoutActor(var generator: IntervalGenerator, target: ActorRef) extends TimeoutActor {
  receiver {
    case Timeout => target ! Timeout
  }
}
