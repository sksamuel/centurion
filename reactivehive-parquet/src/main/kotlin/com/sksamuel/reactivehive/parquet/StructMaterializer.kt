package com.sksamuel.reactivehive.parquet

import com.sksamuel.reactivehive.BinaryType
import com.sksamuel.reactivehive.BooleanType
import com.sksamuel.reactivehive.ByteType
import com.sksamuel.reactivehive.DoubleType
import com.sksamuel.reactivehive.FloatType
import com.sksamuel.reactivehive.IntType
import com.sksamuel.reactivehive.LongType
import com.sksamuel.reactivehive.ShortType
import com.sksamuel.reactivehive.StringType
import com.sksamuel.reactivehive.Struct
import com.sksamuel.reactivehive.StructField
import com.sksamuel.reactivehive.StructType
import org.apache.parquet.column.Dictionary
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter
import org.apache.parquet.io.api.PrimitiveConverter
import org.apache.parquet.io.api.RecordMaterializer

class StructMaterializer(schema: StructType) : RecordMaterializer<Struct>() {

  private val messageConverter = MessageConverter(schema)

  override fun getRootConverter(): GroupConverter = messageConverter

  override fun getCurrentRecord(): Struct = messageConverter.struct!!
}

class MessageConverter(private val schema: StructType) : GroupConverter() {

  private val values = mutableMapOf<String, Any?>()
  internal var struct: Struct? = null

  override fun start() {
    values.clear()
  }

  override fun end() {
    struct = Struct.fromMap(schema, values)
  }

  override fun getConverter(fieldIndex: Int): Converter = Converters.converterFor(schema[fieldIndex])
}

class StructConverter(private val schema: StructType,
                      private val field: StructField,
                      private val parentBuilder: MutableMap<String, Any?>) : GroupConverter() {

  private val values = mutableMapOf<String, Any?>()

  override fun start() {
    values.clear()
  }

  override fun end() {
    parentBuilder[field.name] = values.toMap()
  }

  override fun getConverter(fieldIndex: Int): Converter = Converters.converterFor(schema[fieldIndex])
}


interface Converters {
  companion object {
    fun converterFor(field: StructField): Converter {
      return when (val type = field.type) {
        is StructType -> StructConverter(type, field, mutableMapOf())
        StringType -> DictionaryStringPrimitiveConverter(field, mutableMapOf())
        FloatType, DoubleType, LongType, IntType, BooleanType, ShortType, ByteType, BinaryType ->
          AppendingPrimitiveConverter(field, mutableMapOf())
        else -> throw UnsupportedOperationException("Unsupported data type $type")
      }
    }
  }
}

class DictionaryStringPrimitiveConverter(val field: StructField,
                                         val builder: MutableMap<String, Any?>) : PrimitiveConverter() {

  private var dictionary: Dictionary? = null

  override fun addBinary(x: Binary) {
    builder[field.name] = x.toStringUsingUTF8()
  }

  override fun hasDictionarySupport(): Boolean = true

  override fun setDictionary(dictionary: Dictionary) {
    this.dictionary = dictionary
  }

  override fun addValueFromDictionary(dictionaryId: Int) {
    addBinary(dictionary!!.decodeToBinary(dictionaryId))
  }
}

// reimplementation of Parquet's SimplePrimitiveConverter that places fields into a mutable map
class AppendingPrimitiveConverter(private val field: StructField, private val buffer: MutableMap<String, Any?>) :
    PrimitiveConverter() {

  override fun addBinary(x: Binary) {
    buffer[field.name] = x.bytes
  }

  override fun addBoolean(x: Boolean) {
    buffer[field.name] = x
  }

  override fun addDouble(x: Double) {
    buffer[field.name] = x
  }

  override fun addFloat(x: Float) {
    buffer[field.name] = x
  }

  override fun addInt(x: Int) {
    buffer[field.name] = x
  }

  override fun addLong(x: Long) {
    buffer[field.name] = x
  }
}