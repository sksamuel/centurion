package com.sksamuel.akka.patterns

import java.util.UUID

/** @author Stephen Samuel */
class Envelope(msg: AnyRef) {
  val correlationId = UUID.randomUUID().toString
}
