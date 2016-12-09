package com.sksamuel.akka.patterns

/**
 * create and Envelope message with a current timestamp as default attribute
 */
case class Envelope[T](
  msg: T,
  attributes: Map[Attribute, Any] = Map(
    Timestamp -> System.currentTimeMillis(),
    NanoTime -> System.nanoTime()
  )) {
  def withAttribute(attribute: Attribute, value: Any): Envelope[T] =
    copy(attributes = attributes + (attribute -> value))
}

/**
 * example of attributes that can be used
 */
trait Attribute
case object SourceName extends Attribute
case object Timestamp extends Attribute
case object NanoTime extends Attribute
case object Priority extends Attribute
case object Offset  extends Attribute
case object Sequence extends Attribute
case object CorrelationId extends Attribute
