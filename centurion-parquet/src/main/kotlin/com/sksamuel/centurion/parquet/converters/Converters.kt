package com.sksamuel.centurion.parquet.converters

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.StructBuilder
import org.apache.parquet.io.api.Converter

interface Converters {
  companion object {
    fun converterFor(field: Schema.Field, builder: StructBuilder): Converter {
      return when (val schema = field.schema) {
        is Schema.Struct -> NestedRecordConverter(schema, field, builder)
        Schema.Strings -> DictionaryStringPrimitiveConverter(field.name, builder)
        Schema.Float64, Schema.Float32,
        Schema.Int32, Schema.Int64, Schema.Int16, Schema.Int8, Schema.Bytes, Schema.Booleans ->
          StructBuilderPrimitiveConverter(field.name, builder)
        Schema.TimestampMillis -> TimestampConverter(field.name, builder)
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
