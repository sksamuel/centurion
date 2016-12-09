package com.sksamuel.akka.patterns

import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, OneInstancePerTest}
import akka.testkit.{TestProbe, TestActorRef}
import akka.actor.{ActorRef, ActorSystem}
import scala.concurrent.duration._

/** @author Stephen Samuel */
class TimeoutActorTest extends FlatSpec with MockitoSugar with OneInstancePerTest {

  implicit val system = ActorSystem()
  val probe = TestProbe()

  "a timeout actor" should "terminate after no msg in the timeout period" in {
    val test = TestActorRef(new TestTimeoutActor(new FixedIntervalGenerator(250 millis), probe.ref))
    probe.watch(test)
    probe.expectTerminated(test, 3 seconds)
  }
}

class TestTimeoutActor(var generator: IntervalGenerator, target: ActorRef) extends TimeoutActor {
  receiver {
    case _ =>
  }
}
