package com.sksamuel.akka.patterns

import akka.testkit.{TestProbe, TestActorRef}
import scala.concurrent.duration._

class ResquencerTest extends BaseSpec {

  val probe = TestProbe()
  val reseq = TestActorRef(new Resequencer(probe.ref))

  val msg1 = Envelope("hello").withAttribute(Sequence, 1)
  val msg2 = Envelope("slim").withAttribute(Sequence, 2)
  val msg3 = Envelope("shady").withAttribute(Sequence, 3)

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
