package com.sksamuel.centurion.avro

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.Struct
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

object Records {

  fun toGenericRecord(struct: Struct): GenericData.Record {
    val schema = Schemas.toAvro(struct.schema)
    val record = GenericData.Record(schema)
    struct.iterator().forEach { (field, value) ->
      val encoded = if (value == null) null else Encoders.encoderFor(field.schema).encode(value)
      record.put(field.name, encoded)
    }
    return record
  }
}

fun interface Encoder<T> {
  fun encode(value: Any): T
}

object Encoders {
  fun encoderFor(schema: Schema): Encoder<*> = when (schema) {
    is Schema.Array -> TODO()
    is Schema.Enum -> TODO()
    is Schema.Map -> TODO()
    is Schema.Nullable -> TODO()
    Schema.Booleans -> BooleanEncoder
    Schema.Bytes -> TODO()
    is Schema.Decimal -> TODO()
    Schema.Float32 -> TODO()
    Schema.Float64 -> TODO()
    Schema.Int8 -> Int8Encoder
    Schema.Int16 -> Int16Encoder
    Schema.Int32 -> Int32Encoder
    Schema.Int64 -> Int64Encoder
    Schema.Nulls -> TODO()
    Schema.Strings -> Encoder { Utf8(it.toString()) }
    Schema.TimestampMicros -> TODO()
    Schema.TimestampMillis -> TODO()
    Schema.UUID -> TODO()
    is Schema.Varchar -> TODO()
    is Schema.Struct -> TODO()
  }
}

object Int64Encoder : Encoder<Long> {
  override fun encode(value: Any): Long = when (value) {
    is Long -> value
    is Int -> value.toLong()
    is Short -> value.toLong()
    is Byte -> value.toLong()
    else -> error("Unsupported Long type $value")
  }
}

object Int32Encoder : Encoder<Int> {
  override fun encode(value: Any): Int = when (value) {
    is Int -> value.toInt()
    is Short -> value.toInt()
    is Byte -> value.toInt()
    else -> error("Unsupported Int type $value")
  }
}

object Int16Encoder : Encoder<Int> {
  override fun encode(value: Any): Int = when (value) {
    is Short -> value.toInt()
    is Byte -> value.toInt()
    else -> error("Unsupported Int8 type $value")
  }
}

object Int8Encoder : Encoder<Int> {
  override fun encode(value: Any): Int = when (value) {
    is Byte -> value.toInt()
    else -> error("Unsupported Int8 type $value")
  }
}

object BooleanEncoder : Encoder<Boolean> {
  override fun encode(value: Any): Boolean = when (value) {
    is Boolean -> value
    else -> error("Unsupported Boolean type ${value}")
  }
}
