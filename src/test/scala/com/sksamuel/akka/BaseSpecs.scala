package com.sksamuel.akka
package patterns

import akka.actor.ActorSystem
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import com.typesafe.config.Config
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.{FlatSpec, OneInstancePerTest}

abstract class BaseAsyncSpec(name: String, config: Option[Config] = None) extends TestKit(ActorSystem(name, config))
  with DefaultTimeout with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    shutdown()
  }
}

abstract class BaseSpec extends FlatSpec with OneInstancePerTest with BeforeAndAfterAll with Matchers {
  implicit val system = ActorSystem()

  override def afterAll() = {
    system.terminate()
  }
}
