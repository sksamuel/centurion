package com.sksamuel.reactivehive.parquet.converters

import com.sksamuel.reactivehive.BinaryType
import com.sksamuel.reactivehive.BooleanType
import com.sksamuel.reactivehive.DateType
import com.sksamuel.reactivehive.DecimalType
import com.sksamuel.reactivehive.EnumType
import com.sksamuel.reactivehive.Float32Type
import com.sksamuel.reactivehive.Float64Type
import com.sksamuel.reactivehive.Int16Type
import com.sksamuel.reactivehive.Int32Type
import com.sksamuel.reactivehive.Int64Type
import com.sksamuel.reactivehive.Int8Type
import com.sksamuel.reactivehive.StringType
import com.sksamuel.reactivehive.StructField
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.TimeMillisType
import com.sksamuel.reactivehive.TimestampMillisType
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