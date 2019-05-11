package com.sksamuel.reactivehive.parquet.converters

import com.sksamuel.reactivehive.StructField
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter

// reimplementation of Parquet's SimplePrimitiveConverter that places fields into a mutable map
class AppendingPrimitiveConverter(private val field: StructField, private val buffer: MutableMap<String, Any?>) :
    PrimitiveConverter() {

  override fun addBinary(x: Binary) {
    buffer[field.name] = x.bytes
  }

  override fun addBoolean(x: Boolean) {
    buffer[field.name] = x
  }

  override fun addDouble(x: Double) {
    buffer[field.name] = x
  }

  override fun addFloat(x: Float) {
    buffer[field.name] = x
  }

  override fun addInt(x: Int) {
    buffer[field.name] = x
  }

  override fun addLong(x: Long) {
    buffer[field.name] = x
  }
}