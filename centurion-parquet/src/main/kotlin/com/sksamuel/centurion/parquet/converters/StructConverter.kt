package com.sksamuel.centurion.parquet.converters

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.Struct
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter

open class StructConverter(private val schema: Schema.Struct) : GroupConverter() {

  private val collector = StructValuesCollector(schema.fields.size)
  private var struct: Struct? = null

  private val converters = schema.fields.withIndex().map {
    Converters.converterFor(schema.fields[it.index].schema, it.index, collector)
  }

  // called to start a new group, so we simply clear the map of values
  override fun start() {
    collector.reset()
    struct = null
  }

  override fun end() {
    struct = Struct(schema, collector.values())
  }

  /**
   * Returns the struct that we are building up.
   */
  fun currentStruct(): Struct = struct ?: error("End must have been called to create the struct")

  /**
   * Called at initialization time based on schema.
   * Must consistently return the same object.
   */
  override fun getConverter(fieldIndex: Int): Converter = converters[fieldIndex]
}

class NestedStructConverter(
  schema: Schema.Struct,
  private val index: Int,
  private val collector: ValuesCollector,
) : StructConverter(schema) {
  override fun end() {
    super.end()
    collector[index] = currentStruct()
  }
}
