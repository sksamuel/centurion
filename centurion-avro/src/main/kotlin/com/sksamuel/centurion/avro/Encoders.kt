//package com.sksamuel.centurion.avro
//
//import com.sksamuel.centurion.Schema
//import com.sksamuel.centurion.avro.encoders.BooleanEncoder
//import com.sksamuel.centurion.avro.encoders.ShortEncoder
//import org.apache.avro.util.Utf8
//
//object Encoders {
//  fun encoderFor(schema: Schema): Encoder<*> = when (schema) {
//    is Schema.Array -> TODO()
//    is Schema.Enum -> TODO()
//    is Schema.Map -> TODO()
//    is Schema.Nullable -> TODO()
//    Schema.Booleans -> BooleanEncoder
//    Schema.Bytes -> TODO()
//    is Schema.Decimal -> TODO()
//    Schema.Float32 -> TODO()
//    Schema.Float64 -> TODO()
//    Schema.Int8 -> ShortEncoder
//    Schema.Int16 -> Int16Encoder
//    Schema.Int32 -> Int32Encoder
//    Schema.Int64 -> Int64Encoder
//    Schema.Nulls -> TODO()
//    Schema.Strings -> Encoder { Utf8(it.toString()) }
//    Schema.TimestampMicros -> TODO()
//    Schema.TimestampMillis -> TODO()
//    Schema.UUID -> TODO()
//    is Schema.Varchar -> TODO()
//    is Schema.Struct -> TODO()
//  }
//}
