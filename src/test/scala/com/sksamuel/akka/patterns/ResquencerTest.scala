package com.sksamuel.akka.patterns

import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, OneInstancePerTest}
import akka.testkit.{TestProbe, TestActorRef}
import akka.actor.ActorSystem
import scala.concurrent.duration._

/** @author Stephen Samuel */
class ResquencerTest extends FlatSpec with MockitoSugar with OneInstancePerTest {

  implicit val system = ActorSystem()
  val probe = TestProbe()

  val reseq = TestActorRef(new Resequencer(probe.ref))

  val msg1 = Envelope("hello").withAttribute(SequenceAttribute, 1)
  val msg2 = Envelope("slim").withAttribute(SequenceAttribute, 2)
  val msg3 = Envelope("shady").withAttribute(SequenceAttribute, 3)

  "a resquencer" should "send next message if in order" in {
    reseq ! msg1
    probe.expectMsg(msg1)
  }

  it should "reorder out of order messages" in {
    reseq ! msg2
    reseq ! msg1
    assert(msg1 === probe.receiveOne(3 seconds))
    assert(msg2 === probe.receiveOne(3 seconds))
  }
}
