package com.sksamuel.reactivehive.parquet.converters

import com.sksamuel.reactivehive.StructField
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter

class EnumConverter(val field: StructField,
                    private val buffer: MutableMap<String, Any?>) : PrimitiveConverter() {
  override fun addBinary(binary: Binary) {
    val value = binary.toStringUsingUTF8()
    buffer[field.name] = value
  }
}
