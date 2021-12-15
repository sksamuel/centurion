package com.sksamuel.centurion.avro

object Int8Encoder : Encoder<Int> {
  override fun encode(value: Any): Int = when (value) {
    is Byte -> value.toInt()
    else -> error("Unsupported Int8 type $value")
  }
}
