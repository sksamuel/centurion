package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.schemas.unionNonNullComponent
import org.apache.avro.Schema

/**
 * An [Encoder] that wraps another encoder and handles nullables.
 *
 * The non-null component of the union is resolved once at construction time so
 * that per-encode work is reduced to a null check plus delegation.
 */
class NullEncoder<T>(unionSchema: Schema, private val encoder: Encoder<T>) : Encoder<T?> {

   private val nonNullSchema: Schema

   init {
      require(unionSchema.type == Schema.Type.UNION) { "Nulls can only be encoded with a UNION schema" }
      require(unionSchema.types.size == 2) { "Nulls can only be encoded with a 2 element union schema" }
      nonNullSchema = unionSchema.unionNonNullComponent()
   }

   override fun encode(schema: Schema, value: T?): Any? {
      return if (value == null) null else encoder.encode(nonNullSchema, value)
   }
}
