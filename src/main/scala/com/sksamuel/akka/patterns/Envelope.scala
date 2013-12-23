package com.sksamuel.akka.patterns

/** @author Stephen Samuel */
case class Envelope[T](msg: T,attributes: Map[Attribute, Any] = Map.empty) {
  def withAttribute(attribute: Attribute,
                    value: Any): Envelope[T] = copy(attributes = attributes + (attribute -> value))
}

object Envelope {
  def apply[T](msg: T) = new
      Envelope(msg = msg, attributes = Map[Attribute, Any](MessageTimestampAttribute -> System.currentTimeMillis()))
}

trait Attribute
case object MessageTimestampAttribute extends Attribute
case object PriorityAttribute extends Attribute
case object SequenceAttribute extends Attribute
case object CorrelationId extends Attribute