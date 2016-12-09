package com.sksamuel.akka.patterns

import akka.testkit.{TestProbe, TestActorRef}
import akka.actor.ActorSystem
import scala.concurrent.duration._

class AggregatorTest extends BaseSpec {
  val probe = TestProbe()

  val actorRef = TestActorRef(new Aggregator(probe.ref, classOf[String], classOf[BigDecimal]))

  "an aggregator" should "send message once all components are received" in {
    val correlationId = "abc"
    actorRef ! Envelope("foo").withAttribute(CorrelationId, correlationId)
    actorRef ! Envelope(BigDecimal.valueOf(10d)).withAttribute(CorrelationId, correlationId)
    val msg = probe.receiveOne(5 seconds).asInstanceOf[Seq[_]]
    assert(msg(0).asInstanceOf[Envelope[_]].msg === "foo")
    assert(msg(1).asInstanceOf[Envelope[_]].msg === BigDecimal.valueOf(10d))
  }

  it should "send message with the components in declaration order" in {
    val correlationId = "abc"
    actorRef ! Envelope(BigDecimal.valueOf(10d)).withAttribute(CorrelationId, correlationId)
    actorRef ! Envelope("foo").withAttribute(CorrelationId, correlationId)
    val msg = probe.receiveOne(5 seconds).asInstanceOf[Seq[_]]
    assert(msg(0).asInstanceOf[Envelope[_]].msg === "foo")
    assert(msg(1).asInstanceOf[Envelope[_]].msg === BigDecimal.valueOf(10d))
  }

  it should "buffer components with different correlationIds until a complete message is received" in {
    val correlationId1 = "abc"
    val correlationId2 = "xzy"
    actorRef ! Envelope(BigDecimal.valueOf(10d)).withAttribute(CorrelationId, correlationId1)
    actorRef ! Envelope(BigDecimal.valueOf(5d)).withAttribute(CorrelationId, correlationId2)
    actorRef ! Envelope("bar").withAttribute(CorrelationId, correlationId2)
    actorRef ! Envelope("foo").withAttribute(CorrelationId, correlationId1)
    val msg1 = probe.receiveOne(5 seconds).asInstanceOf[Seq[_]]
    assert(msg1(0).asInstanceOf[Envelope[_]].msg === "bar")
    assert(msg1(1).asInstanceOf[Envelope[_]].msg === BigDecimal.valueOf(5d))
    val msg2 = probe.receiveOne(5 seconds).asInstanceOf[Seq[_]]
    assert(msg2(0).asInstanceOf[Envelope[_]].msg === "foo")
    assert(msg2(1).asInstanceOf[Envelope[_]].msg === BigDecimal.valueOf(10d))
  }
}
