package com.sksamuel.centurion.parquet.converters

import com.sksamuel.centurion.BinaryType
import com.sksamuel.centurion.BooleanType
import com.sksamuel.centurion.DateType
import com.sksamuel.centurion.DecimalType
import com.sksamuel.centurion.EnumType
import com.sksamuel.centurion.Float32Type
import com.sksamuel.centurion.Float64Type
import com.sksamuel.centurion.Int16Type
import com.sksamuel.centurion.Int32Type
import com.sksamuel.centurion.Int64Type
import com.sksamuel.centurion.Int8Type
import com.sksamuel.centurion.StringType
import com.sksamuel.centurion.StructField
import com.sksamuel.centurion.StructType
import com.sksamuel.centurion.TimeMillisType
import com.sksamuel.centurion.TimestampMillisType
import org.apache.parquet.io.api.Converter

interface Converters {
  companion object {
    fun converterFor(field: StructField, buffer: MutableMap<String, Any?>): Converter {
      fun <T> receiver(): Receiver<T> = Receiver(field, buffer)
      return when (val type = field.type) {
        is StructType -> NestedStructConverter(type, field, buffer)
        StringType -> DictionaryStringPrimitiveConverter(receiver())
        Float32Type, Float64Type, Int64Type, Int32Type, BooleanType, Int16Type, Int8Type, BinaryType ->
          AppendingPrimitiveConverter(receiver())
        TimestampMillisType -> TimestampConverter(receiver())
        DateType -> DateConverter(receiver())
        is DecimalType -> DecimalConverter(field, type.precision, type.scale, buffer)
        TimeMillisType -> TimeMillisConverter(receiver())
        is EnumType -> EnumConverter(receiver())
        else -> throw UnsupportedOperationException("Unsupported data type $type")
      }
    }
  }
}

class Receiver<T>(private val field: StructField,
                  private val buffer: MutableMap<String, Any?>) {
  fun add(t: T?) {
    buffer[field.name] = t
  }
}
