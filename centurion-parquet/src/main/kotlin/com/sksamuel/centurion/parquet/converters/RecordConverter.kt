package com.sksamuel.centurion.parquet.converters

import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.StructBuilder
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter

open class RecordConverter(private val schema: Schema.Struct) : GroupConverter() {

  private val builder = StructBuilder(schema)
  private var struct: Struct? = null

  // called to start a new group, so we simply clear the map of values
  override fun start() {
    builder.clear()
    struct = null
  }

  override fun end() {
    struct = builder.toStruct()
  }

  /**
   * Returns the struct that we are building up.
   */
  fun currentStruct(): Struct = struct ?: error("End must have been called to create the struct")

  /**
   * Called at initialization time based on schema.
   * Must consistently return the same object.
   */
  override fun getConverter(fieldIndex: Int): Converter =
    Converters.converterFor(schema.fields[fieldIndex], builder)
}

class NestedRecordConverter(
  schema: Schema.Struct,
  private val field: Schema.Field,
  private val parent: MutableMap<String, Any?>
) : RecordConverter(schema) {
  override fun end() {
    super.end()
    parent[field.name] = currentStruct()
  }
}
