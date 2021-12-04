package com.sksamuel.rxhive.parquet.converters

import com.sksamuel.rxhive.BinaryType
import com.sksamuel.rxhive.BooleanType
import com.sksamuel.rxhive.DateType
import com.sksamuel.rxhive.DecimalType
import com.sksamuel.rxhive.EnumType
import com.sksamuel.rxhive.Float32Type
import com.sksamuel.rxhive.Float64Type
import com.sksamuel.rxhive.Int16Type
import com.sksamuel.rxhive.Int32Type
import com.sksamuel.rxhive.Int64Type
import com.sksamuel.rxhive.Int8Type
import com.sksamuel.rxhive.StringType
import com.sksamuel.rxhive.StructField
import com.sksamuel.rxhive.StructType
import com.sksamuel.rxhive.TimeMillisType
import com.sksamuel.rxhive.TimestampMillisType
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