package com.sksamuel.reactivehive.parquet

import com.sksamuel.reactivehive.BinaryType
import com.sksamuel.reactivehive.BooleanType
import com.sksamuel.reactivehive.DateType
import com.sksamuel.reactivehive.DecimalType
import com.sksamuel.reactivehive.Float32Type
import com.sksamuel.reactivehive.Float64Type
import com.sksamuel.reactivehive.Int16Type
import com.sksamuel.reactivehive.Int32Type
import com.sksamuel.reactivehive.Int64Type
import com.sksamuel.reactivehive.Int8Type
import com.sksamuel.reactivehive.StringType
import com.sksamuel.reactivehive.Struct
import com.sksamuel.reactivehive.StructField
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.TimestampMillisType
import com.sksamuel.reactivehive.parquet.converters.AppendingPrimitiveConverter
import com.sksamuel.reactivehive.parquet.converters.DateConverter
import com.sksamuel.reactivehive.parquet.converters.DecimalConverter
import com.sksamuel.reactivehive.parquet.converters.DictionaryStringPrimitiveConverter
import com.sksamuel.reactivehive.parquet.converters.TimestampConverter
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter
import org.apache.parquet.io.api.RecordMaterializer

class StructMaterializer(schema: StructType) : RecordMaterializer<Struct>() {

  private val rootConverter = StructConverter(schema)

  override fun getRootConverter(): GroupConverter = rootConverter

  override fun getCurrentRecord(): Struct = rootConverter.struct!!
}

open class StructConverter(private val schema: StructType) : GroupConverter() {

  private val buffer = mutableMapOf<String, Any?>()
  internal var struct: Struct? = null

  // called to start a new group, so we simply clear the map
  override fun start() {
    buffer.clear()
    struct = null
  }

  override fun end() {
    struct = Struct.fromMap(schema, buffer.toMap())
  }

  override fun getConverter(fieldIndex: Int): Converter = Converters.converterFor(schema[fieldIndex], buffer)
}

class NestedStructConverter(schema: StructType,
                            private val field: StructField,
                            private val parent: MutableMap<String, Any?>) : StructConverter(schema) {
  override fun end() {
    super.end()
    parent[field.name] = struct
  }
}

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
        else -> throw UnsupportedOperationException("Unsupported data type $type")
      }
    }
  }
}


