package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.decoders.StringDecoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericData

/**
 * An [Encoder] for Arrays of [T] that encodes into an Avro [GenericArray].
 */
class ArrayEncoder<T>(private val encoder: Encoder<T>) : Encoder<Array<T>> {
   override fun encode(schema: Schema, value: Array<T>): Any {
      require(schema.type == Schema.Type.ARRAY)
      val elements = value.map { encoder.encode(schema.elementType, it) }
      return GenericData.Array(schema, elements)
   }
}

/**
 * An [Encoder] for Lists of [T] that encodes into an Avro [GenericArray].
 */
class ListEncoder<T>(private val encoder: Encoder<T>) : Encoder<List<T>> {
   override fun encode(schema: Schema, value: List<T>): Any {
      require(schema.type == Schema.Type.ARRAY)
      if (value.isEmpty()) return GenericData.Array<T>(0, schema)
      val elements = value.map { encoder.encode(schema.elementType, it) }
      return GenericData.Array(schema, elements)
   }
}

/**
 * An [Encoder] for Sets of [T] that encodes into an Avro [GenericArray].
 */
class SetEncoder<T>(private val encoder: Encoder<T>) : Encoder<Set<T>> {
   override fun encode(schema: Schema, value: Set<T>): GenericArray<Any> {
      require(schema.type == Schema.Type.ARRAY)
      if (value.isEmpty()) return GenericData.Array(0, schema)
      val elements = value.map { encoder.encode(schema.elementType, it) }
      return GenericData.Array(schema, elements)
   }
}

class MapEncoder<T>(
   private val keyEncoder: Encoder<String>,
   private val valueEncoder: Encoder<T>
) : Encoder<Map<String, T>> {
   override fun encode(schema: Schema, value: Map<String, T>): Map<Any?, Any?> {
      require(schema.type == Schema.Type.MAP)
      if (value.isEmpty()) return emptyMap()
      return value.map { (key, value) ->
         keyEncoder.encode(StringDecoder.STRING_SCHEMA, key) to valueEncoder.encode(schema.valueType, value)
      }.toMap()
   }
}
