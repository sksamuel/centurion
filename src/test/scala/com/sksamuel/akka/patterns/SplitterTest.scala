package com.sksamuel.akka.patterns

import org.scalatest.mockito.MockitoSugar
import org.scalatest.{OneInstancePerTest, FunSuite}
import akka.testkit.{TestProbe, TestActorRef}
import akka.actor.ActorSystem

/** @author Stephen Samuel */
class SplitterTest extends FunSuite with MockitoSugar with OneInstancePerTest {

  implicit val system = ActorSystem()
  val probe = TestProbe()
  val actorRef = TestActorRef(new Splitter(probe.ref))

  test("splitter will split array") {
    actorRef ! Array("foo", "bar", "baz")
    val received = probe.receiveN(3)
    assert("foo" === received(0))
    assert("bar" === received(1))
    assert("baz" === received(2))
  }

  test("splitter will split set") {
    actorRef ! Set("foo", "bar", "baz")
    val received = probe.receiveN(3)
    assert("foo" === received(0))
    assert("bar" === received(1))
    assert("baz" === received(2))
  }

  test("splitter will split list") {
    actorRef ! List("foo", "bar", "baz")
    val received = probe.receiveN(3)
    assert("foo" === received(0))
    assert("bar" === received(1))
    assert("baz" === received(2))
  }

  test("splitter will split iterator") {
    actorRef ! Iterator("foo", "bar", "baz")
    val received = probe.receiveN(3)
    assert("foo" === received(0))
    assert("bar" === received(1))
    assert("baz" === received(2))
  }
}
