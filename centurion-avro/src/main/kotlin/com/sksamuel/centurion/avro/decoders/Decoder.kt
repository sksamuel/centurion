package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema

/**
 * A [Decoder] typeclass is used to convert an Avro value, such as a [GenericRecord],
 * [SpecificRecord], [GenericFixed], [EnumSymbol], or a basic type, into a specified JVM type.
 *
 * For example, a [Decoder<String>] would convert an input such as a GenericFixed, byte array, or Utf8
 * into a plain JVM [String].
 *
 * Another example, a decoder for nullable types would handle null-based Unions.
 */
fun interface Decoder<T> {

   fun decode(schema: Schema, value: Any?): T

   fun <U> map(fn: (T) -> U): Decoder<U> {
      val self = this
      return Decoder { schema, value -> fn(self.decode(schema, value)) }
   }
}

//object Decoders {
//   fun decoderFor(schema: Schema): Decoder<*> = when (schema) {
//      is Schema.Array -> ListDecoder(schema.elements)
//      is Schema.Enum -> TODO()
//      is Schema.Map -> TODO()
//      is Schema.Nullable -> TODO()
//      Schema.Booleans -> BooleanDecoder
//      Schema.Bytes -> TODO()
//      is Schema.Decimal -> TODO()
//      Schema.Float32 -> TODO()
//      Schema.Float64 -> TODO()
//      Schema.Int8 -> TODO()
//      Schema.Int16 -> TODO()
//      Schema.Int32 -> Int32Decoder
//      Schema.Int64 -> Int64Decoder
//      Schema.Nulls -> TODO()
//      Schema.Strings -> StringDecoder
//      Schema.TimestampMicros -> TODO()
//      Schema.TimestampMillis -> TODO()
//      Schema.UUID -> TODO()
//      is Schema.Varchar -> TODO()
//      is Schema.Struct -> TODO()
//   }
//}
