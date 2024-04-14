package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema

/**
 * An [Encoder] that supports nullable types
 */
class NullEncoder<T>(private val encoder: Encoder<T>) : Encoder<T?> {
   override fun encode(schema: Schema, value: T?): Any? {
      // nullables must be encoded with a union of 2 elements, where null is the first type
      require(schema.type == Schema.Type.UNION) { "Nulls can only be encoded with a UNION schema" }
      require(schema.types.size == 2) { "Nulls can only be encoded with a 2 element union schema" }
      return if (value == null) null else encoder.encode(schema.types[1], value)
   }
}
