package com.sksamuel.centurion.avro

object Int16Encoder : Encoder<Int> {
  override fun encode(value: Any): Int = when (value) {
    is Short -> value.toInt()
    is Byte -> value.toInt()
    else -> error("Unsupported Int8 type $value")
  }
}
