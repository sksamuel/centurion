package com.sksamuel.rxhive.parquet.converters

import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter

class EnumConverter(private val receiver: Receiver<Any>) : PrimitiveConverter() {
  override fun addBinary(binary: Binary) {
    val value = binary.toStringUsingUTF8()
    receiver.add(value)
  }
}
