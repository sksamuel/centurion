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
      return when (val type = field.type) {
        is StructType -> NestedStructConverter(type, field, buffer)
        StringType -> DictionaryStringPrimitiveConverter(field, buffer)
        Float32Type, Float64Type, Int64Type, Int32Type, BooleanType, Int16Type, Int8Type, BinaryType ->
          AppendingPrimitiveConverter(field, buffer)
        TimestampMillisType -> TimestampConverter(field, buffer)
        DateType -> DateConverter(field, buffer)
        is DecimalType -> DecimalConverter(field, type.precision, type.scale, buffer)
        TimeMillisType -> TimeMillisConverter(field, buffer)
        is EnumType -> EnumConverter(field, buffer)
        else -> throw UnsupportedOperationException("Unsupported data type $type")
      }
    }
  }
}