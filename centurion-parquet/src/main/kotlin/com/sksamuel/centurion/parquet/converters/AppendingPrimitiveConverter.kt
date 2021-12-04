package com.sksamuel.centurion.parquet.converters

import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter

// reimplementation of Parquet's SimplePrimitiveConverter that places fields into a mutable map
class AppendingPrimitiveConverter(private val receiver: Receiver<Any>) : PrimitiveConverter() {

  override fun addBinary(x: Binary) {
    receiver.add(x.bytes)
  }

  override fun addBoolean(x: Boolean) {
    receiver.add(x)
  }

  override fun addDouble(x: Double) {
    receiver.add(x)
  }

  override fun addFloat(x: Float) {
    receiver.add(x)
  }

  override fun addInt(x: Int) {
    receiver.add(x)
  }

  override fun addLong(x: Long) {
    receiver.add(x)
  }
}
