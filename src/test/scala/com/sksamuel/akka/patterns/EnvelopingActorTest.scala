package com.sksamuel.akka.patterns

import akka.testkit.{TestProbe, TestActorRef}
import scala.concurrent.duration._
import java.util.UUID

object AttributesGenerator {
  var offsetCounter = 0
  def generate(x: Any) = {
    x match {
      case s: String =>
        offsetCounter +=1
        Map(
          Offset -> offsetCounter,
          CorrelationId -> UUID.randomUUID().toString(),
          SourceName -> "Generator"
        )
    }
  }

  val anonymousGenerate = (x: Any) => x match {
      case s: String =>
        offsetCounter += 1
        Map(
          Offset -> offsetCounter,
          CorrelationId -> UUID.randomUUID().toString(),
          SourceName -> "Generator"
        )
    }
}

class EnvelopingActorTest extends BaseSpec {
  val probe = TestProbe()

  "An Enveloper Actor" should "envelope messages with a timestamp as default" in {
    val enveloper = TestActorRef(new EnvelopingActor(probe.ref))
    enveloper ! "hello"
    val msg1 = probe.expectMsgType[Envelope[String]]
    msg1.attributes.get(Timestamp) shouldBe defined

    enveloper ! "test"
    val msg2 = probe.expectMsgType[Envelope[String]]
    msg2.attributes.get(Timestamp) shouldBe defined
  }

  it should "envelope messages with the attributes defined by the Attribute Enricher function" in {
    val enveloper = TestActorRef(new EnvelopingActor(probe.ref, AttributesGenerator.generate))
    enveloper ! "hello"
    val msg1 = probe.expectMsgType[Envelope[String]]
    msg1.attributes.get(Timestamp) shouldBe defined
    msg1.attributes.get(Offset) shouldBe defined
    msg1.attributes.get(CorrelationId) shouldBe defined

    enveloper ! "test"
    val msg2 = probe.expectMsgType[Envelope[String]]
    msg2.attributes.get(Timestamp) shouldBe defined
    msg1.attributes.get(Offset) shouldBe defined
    msg1.attributes.get(CorrelationId) shouldBe defined

    //println(msg1)
    //println(msg2)
  }

  it should "envelope consecutive messages with consecutive timestamps" in {
    val enveloper = TestActorRef(new EnvelopingActor(probe.ref))
    enveloper ! "hello"
    val msg1 = probe.expectMsgType[Envelope[String]]
    msg1.attributes.get(Timestamp) shouldBe defined

    enveloper ! "test"
    val msg2 = probe.expectMsgType[Envelope[String]]
    msg2.attributes.get(Timestamp) shouldBe defined

    msg1.attributes(Timestamp).asInstanceOf[Long] should be <= msg2.attributes(Timestamp).asInstanceOf[Long]
    msg1.attributes(NanoTime).asInstanceOf[Long] should be < msg2.attributes(NanoTime).asInstanceOf[Long]
  }

}
