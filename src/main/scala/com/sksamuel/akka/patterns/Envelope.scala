package com.sksamuel.akka.patterns

import java.util.UUID

/** @author Stephen Samuel */
case class Envelope[T](msg: T, correlationId: String = UUID.randomUUID().toString) {
  val timestamp = System.currentTimeMillis()
}
