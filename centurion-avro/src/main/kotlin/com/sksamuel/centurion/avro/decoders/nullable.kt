package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema

class NullDecoder<T>(private val decoder: Decoder<T>) : Decoder<T?> {
   override fun decode(schema: Schema, value: Any?): T? {
      // nullables must be encoded with a union of 2 elements, where null is the first type
      require(schema.type == Schema.Type.UNION) { "Nulls can only be encoded with a UNION schema" }
      val types = schema.types
      require(types.size == 2) { "Nulls can only be encoded with a 2 element union schema" }
      if (value == null) return null
      val nonNullType = when {
         types[0].type == Schema.Type.NULL -> types[1]
         types[1].type == Schema.Type.NULL -> types[0]
         else -> error("One of the elements of a nullable union must not be null")
      }
      return decoder.decode(nonNullType, value)
   }
}
