package com.sksamuel.akka.patterns

import org.scalatest.{OneInstancePerTest, FunSuite}
import org.scalatest.mockito.MockitoSugar
import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestProbe}
import scala.concurrent.duration._

/** @author Stephen Samuel */
class MessageFilterTest extends FunSuite with MockitoSugar with OneInstancePerTest {

  implicit val system = ActorSystem()
  val probe = TestProbe()
  val actorRef = TestActorRef(new MessageFilter((x: Any) => x.isInstanceOf[String], probe.ref))

  test("message filter accepts messages that meet filter criteria") {
    actorRef ! new Object
    probe.expectNoMsg(1 seconds)
    actorRef ! "foo"
    val msg = probe.expectMsg("foo")
    assert("foo" === msg)
  }
}
