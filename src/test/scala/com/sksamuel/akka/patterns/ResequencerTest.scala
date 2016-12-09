package com.sksamuel.akka.patterns

import akka.testkit.{TestProbe, TestActorRef}
import scala.concurrent.duration._

class ResequencerTest extends BaseSpec {

  val probe = TestProbe()


  val msg1 = Envelope("hello").withAttribute(Sequence, 1)
  val msg2 = Envelope("slim").withAttribute(Sequence, 2)
  val msg3 = Envelope("shady").withAttribute(Sequence, 3)

  "a Resquencer" should "send next message if in order" in {
    val reseq = TestActorRef(new Resequencer(probe.ref))
    reseq ! msg1
    probe.expectMsg(msg1)
  }

  it should "reorder out of order messages" in {
    val reseq = TestActorRef(new Resequencer(probe.ref))
    reseq ! msg3
    reseq ! msg2
    reseq ! msg1

    probe.expectMsg(1 second, msg1)
    probe.expectMsg(1 second, msg2)
    probe.expectMsg(1 second, msg3)
  }
}
