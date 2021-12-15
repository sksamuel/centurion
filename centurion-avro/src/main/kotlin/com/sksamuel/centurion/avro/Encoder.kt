package com.sksamuel.centurion.avro

fun interface Encoder<T> {
  fun encode(value: Any): T
}
