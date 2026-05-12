package com.sksamuel.centurion.avro.decoders

import com.sksamuel.centurion.avro.schemas.unionNonNullComponent
import org.apache.avro.Schema

/**
 * A [Decoder] that wraps another decoder and handles nullables.
 *
 * The non-null component of the union is resolved once at construction time so
 * that per-decode work is reduced to a null check plus delegation.
 */
class NullDecoder<T>(unionSchema: Schema, private val decoder: Decoder<T>) : Decoder<T?> {

   private val nonNullSchema: Schema

   init {
      require(unionSchema.type == Schema.Type.UNION) { "Nulls can only be encoded with a UNION schema" }
      require(unionSchema.types.size == 2) { "Nulls can only be encoded with a 2 element union schema" }
      nonNullSchema = unionSchema.unionNonNullComponent()
   }

   override fun decode(schema: Schema, value: Any?): T? {
      return if (value == null) null else decoder.decode(nonNullSchema, value)
   }
}
