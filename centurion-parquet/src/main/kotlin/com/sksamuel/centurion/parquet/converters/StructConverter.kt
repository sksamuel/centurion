package com.sksamuel.centurion.parquet.converters

import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.StructType
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter

open class StructConverter(private val schema: StructType) : GroupConverter() {

  private val buffer = mutableMapOf<String, Any?>()
  private var struct: Struct? = null

  // called to start a new group, so we simply clear the map
  override fun start() {
    buffer.clear()
    struct = null
  }

  override fun end() {
    struct = Struct.fromMap(schema, buffer.toMap())
  }

  fun currentStruct(): Struct = struct!!

  override fun getConverter(fieldIndex: Int): Converter = Converters.converterFor(schema[fieldIndex], buffer)
}
