package com.sksamuel.akka.patterns

import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, OneInstancePerTest}
import akka.testkit.{TestProbe, TestActorRef}
import akka.actor.ActorSystem
import scala.concurrent.duration._

/** @author Stephen Samuel */
class ContentRouterTest extends FlatSpec with MockitoSugar with OneInstancePerTest {

  implicit val system = ActorSystem()
  val probe1 = TestProbe()
  val probe2 = TestProbe()
  val route1 = Route((x: String) => probe1.ref ! x)
  val route2 = Route((x: BigDecimal) => probe2.ref ! x)

  val actorRef = TestActorRef(new ContentRouter(route1, route2))

  "a content router" should "send messages based on class registration" in {
    actorRef ! "foo"
    probe1.expectMsg("foo")
    actorRef ! BigDecimal(10)
    probe2.expectMsg(BigDecimal(10))
  }

  "a content router" should "ignore non registered types" in {
    actorRef ! new Object
    probe1.expectNoMsg(1 seconds)
    probe2.expectNoMsg(1 seconds)
  }
}
