package com.sksamuel.centurion.parquet.converters

import com.sksamuel.centurion.Schema
import org.apache.parquet.io.api.Converter

interface Converters {
  companion object {

    fun converterFor(schema: Schema, index: Int, collector: ValuesCollector): Converter {
      return when (schema) {
        is Schema.Struct -> NestedStructConverter(schema, index, collector)
        Schema.Strings -> DictionaryStringPrimitiveConverter(index, collector)
        Schema.Float64, Schema.Float32,
        Schema.Int32, Schema.Int64, Schema.Int16, Schema.Int8, Schema.Bytes, Schema.Booleans ->
          StructBuilderPrimitiveConverter(index, collector)
        Schema.TimestampMillis -> TimestampConverter(index, collector)
        is Schema.Nullable -> converterFor(schema.element, index, collector)
        is Schema.Map -> MapConverter(schema, index, collector)
        is Schema.Array -> ArrayConverter(schema, index, collector)
//        DateType -> DateConverter(receiver())
//        is DecimalType -> DecimalConverter(field, type.precision, type.scale, buffer)
//        Schema.TimestampMillis -> TimeMillisConverter(field.name, builder)
//        is EnumType -> EnumConverter(receiver())
        else -> throw error("Unsupported schema ${schema::class}")
      }
    }
  }
}

class Receiver<T>(
  private val field: Schema.Field,
  private val buffer: MutableMap<String, Any?>
) {
  fun add(t: T?) {
    buffer[field.name] = t
  }
}
