package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema

class NullDecoder<T>(private val decoder: Decoder<T>) : Decoder<T?> {
   override fun decode(schema: Schema, value: Any?): T? {
      // nullables must be encoded with a union of 2 elements, where null is the first type
      require(schema.type == Schema.Type.UNION) { "Nulls can only be encoded with a UNION schema" }
      require(schema.types.size == 2) { "Nulls can only be encoded with a 2 element union schema" }
      val nullableType = schema.types.firstOrNull { !it.isNullable }
         ?: error("One of the elements of a nullable union must not be null")
      return if (value == null) null else {
         decoder.decode(nullableType, value)
      }
   }
}
