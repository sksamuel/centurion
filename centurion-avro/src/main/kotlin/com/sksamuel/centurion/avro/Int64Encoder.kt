package com.sksamuel.centurion.avro

object Int64Encoder : Encoder<Long> {
  override fun encode(value: Any): Long = when (value) {
    is Long -> value
    is Int -> value.toLong()
    is Short -> value.toLong()
    is Byte -> value.toLong()
    else -> error("Unsupported Long type $value")
  }
}
