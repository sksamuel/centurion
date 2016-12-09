package com.sksamuel.akka.router

import akka.actor._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, OneInstancePerTest}

/** @author Stephen Samuel */
class OneTimeRouterTest extends FlatSpec with MockitoSugar with OneInstancePerTest {

  //  implicit val system = ActorSystem()
  //  val probe = TestProbe()
  //  val router = system.actorOf(Props(classOf[Routee], probe.ref).withRouter(new OneTimeRouter))
  //
  //  "a one time router" should "not reuse the same routee" in {
  //    router ! "test"
  //    probe.expectMsg("test")
  //    val sender1 = probe.lastSender
  //
  //    router ! "test"
  //    probe.expectMsg("test")
  //    val sender2 = probe.lastSender
  //
  //    assert(sender1 != sender2)
  //  }
}

class Routee(target: ActorRef) extends Actor {
  override def preStart() {
    context.watch(self)
  }
  def receive = {
    case msg => target ! msg
  }
}
