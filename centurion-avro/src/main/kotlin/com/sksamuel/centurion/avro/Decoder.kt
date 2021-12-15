package com.sksamuel.centurion.avro

import com.sksamuel.centurion.Schema
import org.apache.avro.util.Utf8
import java.sql.Timestamp

interface Decoder<T> {
  fun decode(any: Any?): T
}

object Decoders {
  fun decoderFor(schema: Schema): Decoder<*> = when (schema) {
    is Schema.Array -> ListDecoder(schema.elements)
    is Schema.Enum -> TODO()
    is Schema.Map -> TODO()
    is Schema.Nullable -> TODO()
    Schema.Booleans -> BooleanDecoder
    Schema.Bytes -> TODO()
    is Schema.Decimal -> TODO()
    Schema.Float32 -> TODO()
    Schema.Float64 -> TODO()
    Schema.Int8 -> TODO()
    Schema.Int16 -> TODO()
    Schema.Int32 -> Int32Decoder
    Schema.Int64 -> Int64Decoder
    Schema.Nulls -> TODO()
    Schema.Strings -> StringDecoder
    Schema.TimestampMicros -> TODO()
    Schema.TimestampMillis -> TODO()
    Schema.UUID -> TODO()
    is Schema.Varchar -> TODO()
    is Schema.Struct -> TODO()
  }
}

class ListDecoder(schema: Schema) : Decoder<List<*>> {
  private val decoder = Decoders.decoderFor(schema)
  override fun decode(any: Any?): List<*> {
    return when (any) {
      is List<*> -> any.map { decoder.decode(it) }
      else -> error("Cannot coerce type ${any!!::class.simpleName} to List<*>")
    }
  }

}

object StringDecoder : Decoder<String> {
  override fun decode(any: Any?): String {
    return when (any) {
      is Utf8 -> any.toString()
      is String -> any
      else -> any.toString()
    }
  }
}

object BooleanDecoder : Decoder<Boolean> {
  override fun decode(any: Any?): Boolean {
    return when (any) {
      is Boolean -> any
      else -> error("Cannot coerce type ${any!!::class.simpleName} to Boolean")
    }
  }
}

object Int32Decoder : Decoder<Int> {
  override fun decode(any: Any?): Int {
    return when (any) {
      is Int -> any
      else -> error("Cannot coerce type ${any!!::class.simpleName} to Int")
    }
  }
}

object Int64Decoder : Decoder<Long> {
  override fun decode(any: Any?): Long {
    return when (any) {
      is Long -> any
      is Int -> any.toLong()
      else -> error("Cannot coerce type ${any!!::class.simpleName} to Long")
    }
  }
}

object TimestampDecoder : Decoder<Timestamp> {
  override fun decode(any: Any?): Timestamp {
    return when (any) {
      is Long -> Timestamp(any)
      is Timestamp -> any
      else -> error("Cannot coerce type ${any!!::class.simpleName} to Long")
    }
  }
}
