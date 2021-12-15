package com.sksamuel.centurion.avro

object Int32Encoder : Encoder<Int> {
  override fun encode(value: Any): Int = when (value) {
    is Int -> value.toInt()
    is Short -> value.toInt()
    is Byte -> value.toInt()
    else -> error("Unsupported Int type $value")
  }
}
