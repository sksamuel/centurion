package com.sksamuel.centurion.avro

object BooleanEncoder : Encoder<Boolean> {
  override fun encode(value: Any): Boolean = when (value) {
    is Boolean -> value
    else -> error("Unsupported Boolean type ${value}")
  }
}
