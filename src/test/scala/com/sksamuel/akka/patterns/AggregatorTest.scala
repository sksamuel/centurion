package com.sksamuel.akka.patterns

import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, OneInstancePerTest}
import akka.testkit.{TestProbe, TestActorRef}
import akka.actor.ActorSystem
import scala.concurrent.duration._

/** @author Stephen Samuel */
class AggregatorTest extends FlatSpec with MockitoSugar with OneInstancePerTest {

  implicit val system = ActorSystem()
  val probe = TestProbe()

  val actorRef = TestActorRef(new Aggregator(probe.ref, classOf[String], classOf[BigDecimal]))

  "an aggregator" should "send message once all components are received" in {
    val correlationId = "abc"
    actorRef ! Envelope("foo", correlationId)
    actorRef ! Envelope(BigDecimal.valueOf(10d), correlationId)
    val msg = probe.receiveOne(5 seconds).asInstanceOf[Seq[_]]
    assert(msg(0) === "foo")
    assert(msg(1) === BigDecimal.valueOf(10d))
  }

  "an aggregator" should "send message with the components in declaration order" in {
    val correlationId = "abc"
    actorRef ! Envelope(BigDecimal.valueOf(10d), correlationId)
    actorRef ! Envelope("foo", correlationId)
    val msg = probe.receiveOne(5 seconds).asInstanceOf[Seq[_]]
    assert(msg(0) === "foo")
    assert(msg(1) === BigDecimal.valueOf(10d))
  }

  "an aggregator" should "buffer components with different correlationIds until a complete message is received" in {
    val correlationId1 = "abc"
    val correlationId2 = "xzy"
    actorRef ! Envelope(BigDecimal.valueOf(10d), correlationId1)
    actorRef ! Envelope(BigDecimal.valueOf(5d), correlationId2)
    actorRef ! Envelope("bar", correlationId2)
    actorRef ! Envelope("foo", correlationId1)
    val msg1 = probe.receiveOne(5 seconds).asInstanceOf[Seq[_]]
    assert(msg1(0) === "bar")
    assert(msg1(1) === BigDecimal.valueOf(5d))
    val msg2 = probe.receiveOne(5 seconds).asInstanceOf[Seq[_]]
    assert(msg2(0) === "foo")
    assert(msg2(1) === BigDecimal.valueOf(10d))
  }
}
