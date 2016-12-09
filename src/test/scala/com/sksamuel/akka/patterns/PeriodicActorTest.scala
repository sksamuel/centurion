package com.sksamuel.akka.patterns

import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, OneInstancePerTest}
import akka.testkit.{TestProbe, TestActorRef}
import akka.actor.{ActorRef, ActorSystem}
import scala.concurrent.duration._

/** @author Stephen Samuel */
class PeriodicActorTest extends FlatSpec with MockitoSugar with OneInstancePerTest {

  implicit val system = ActorSystem()
  val probe = TestProbe()

  "a periodic actor" should "send a tick at every interval" in {
    TestActorRef(new TestPeriodicActor(new FixedIntervalGenerator(1 second), probe.ref))
    probe.receiveN(2, 2500 millis)
  }
}

class TestPeriodicActor(var generator: IntervalGenerator, target: ActorRef) extends PeriodicActor {
  receiver {
    case Tick => target ! "received"
  }
}
