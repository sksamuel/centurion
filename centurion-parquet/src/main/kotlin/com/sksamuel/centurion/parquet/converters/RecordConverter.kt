package com.sksamuel.centurion.parquet.converters

import com.sksamuel.centurion.Record
import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.StructBuilder
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter

open class RecordConverter(private val schema: Schema.Record) : GroupConverter() {

  private val builder = StructBuilder(schema)
  private var record: Record? = null

  // called to start a new group, so we simply clear the map of values
  override fun start() {
    builder.clear()
    record = null
  }

  override fun end() {
    record = builder.toStruct()
  }

  /**
   * Returns the struct that we are building up.
   */
  fun currentStruct(): Record = record ?: error("End must have been called to create the struct")

  /**
   * Called at initialization time based on schema.
   * Must consistently return the same object.
   */
  override fun getConverter(fieldIndex: Int): Converter =
    Converters.converterFor(schema.fields[fieldIndex], builder)
}

class NestedRecordConverter(
  schema: Schema.Record,
  private val field: Schema.Field,
  private val parent: MutableMap<String, Any?>
) : RecordConverter(schema) {
  override fun end() {
    super.end()
    parent[field.name] = currentStruct()
  }
}
