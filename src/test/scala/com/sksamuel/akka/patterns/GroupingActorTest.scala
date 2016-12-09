package com.sksamuel.akka.patterns

import org.scalatest.mockito.MockitoSugar
import org.scalatest.{OneInstancePerTest, FunSuite}
import akka.testkit.{TestProbe, TestActorRef}
import akka.actor.ActorSystem
import scala.concurrent.duration._

/** @author Stephen Samuel */
class GroupingActorTest extends FunSuite with MockitoSugar with OneInstancePerTest {

  implicit val system = ActorSystem()
  val probe = TestProbe()

  test("counting latch waits for specified number of messages") {
    val actorRef = TestActorRef(new GroupingActor(3, probe.ref))

    actorRef ! new Object
    probe.expectNoMsg(1 seconds)
    actorRef ! new Object
    probe.expectNoMsg(1 seconds)
    actorRef ! new Object
    probe.receiveN(1)
  }

  test("counting latch sends all pending messages in order") {
    val actorRef = TestActorRef(new GroupingActor(3, probe.ref))

    actorRef ! "foo"
    actorRef ! "bar"
    actorRef ! "baz"
    val received = probe.expectMsgClass(classOf[List[_]])
    assert(received.size === 3)
    assert("foo" === received(0))
    assert("bar" === received(1))
    assert("baz" === received(2))
  }
}
