package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericArray

/**
 * An [Encoder] for Arrays of [T].
 */
class ArrayEncoder<T>(private val encoder: Encoder<T>) : Encoder<Array<T>> {
   override fun encode(schema: Schema, value: Array<T>): List<Any?> {
      require(schema.type == Schema.Type.ARRAY)
      return if (value.isEmpty()) emptyList()
      else value.map { encoder.encode(schema.elementType, it) }
   }
}

/**
 * An [Encoder] for LongArrays.
 */
class LongArrayEncoder : Encoder<LongArray> {
   override fun encode(schema: Schema, value: LongArray): List<Long> {
      require(schema.type == Schema.Type.ARRAY)
      return value.asList()
   }
}

/**
 * An [Encoder] for IntArrays.
 */
class IntArrayEncoder : Encoder<IntArray> {
   override fun encode(schema: Schema, value: IntArray): List<Int> {
      require(schema.type == Schema.Type.ARRAY)
      return value.asList()
   }
}

/**
 * An [Encoder] for Lists of [T] that encodes into an Avro [GenericArray].
 */
class ListEncoder<T>(private val encoder: Encoder<T>) : Encoder<List<T>> {
   override fun encode(schema: Schema, value: List<T>): List<Any?> {
      require(schema.type == Schema.Type.ARRAY)
      return if (value.isEmpty()) emptyList()
      else value.map { encoder.encode(schema.elementType, it) }
   }
}

/**
 * An [Encoder] for Sets of [T] that encodes into an Avro [GenericArray].
 */
class SetEncoder<T>(private val encoder: Encoder<T>) : Encoder<Set<T>> {
   override fun encode(schema: Schema, value: Set<T>): List<Any?> {
      require(schema.type == Schema.Type.ARRAY)
      return if (value.isEmpty()) emptyList()
      else value.map { encoder.encode(schema.elementType, it) }
   }
}

class MapEncoder<T>(
   private val valueEncoder: Encoder<T>
) : Encoder<Map<String, T>> {
   override fun encode(schema: Schema, value: Map<String, T>): Map<String, Any?> {
      require(schema.type == Schema.Type.MAP)
      return if (value.isEmpty()) emptyMap()
      else value.map { (key, value) ->
         key.toString() to valueEncoder.encode(schema.valueType, value)
      }.toMap()
   }
}
