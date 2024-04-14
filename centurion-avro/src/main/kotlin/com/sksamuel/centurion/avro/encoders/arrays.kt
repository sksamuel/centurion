package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.Encoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

/**
 * An [Encoder] for Arrays of [T] that encodes into an Avro [GenericArray].
 */
class GenericArrayArrayEncoder<T>(private val encoder: Encoder<T>) : Encoder<Array<T>> {
   override fun encode(schema: Schema, value: Array<T>): Any {
      require(schema.type == Schema.Type.ARRAY)
      val elements = value.map { encoder.encode(schema.elementType, it) }
      return GenericData.Array<T>(elements.size, schema.elementType)
   }
}

/**
 * An [Encoder] for Lists of [T] that encodes into an Avro [GenericArray].
 */
class GenericArrayListEncoder<T>(private val encoder: Encoder<T>) : Encoder<List<T>> {
   override fun encode(schema: Schema, value: List<T>): Any {
      require(schema.type == Schema.Type.ARRAY)
      val elements = value.map { encoder.encode(schema.elementType, it) }
      return GenericData.Array<T>(elements.size, schema.elementType)
   }
}
