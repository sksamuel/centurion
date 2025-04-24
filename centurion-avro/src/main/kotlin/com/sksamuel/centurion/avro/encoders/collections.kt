package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema

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
 * An [Encoder] for Lists of [T] that encodes each element using a delegated [encoder].
 */
class ListEncoder<T>(private val encoder: Encoder<T>) : Encoder<List<T>> {
   override fun encode(schema: Schema, value: List<T>): List<Any?> {
      require(schema.type == Schema.Type.ARRAY)
      return if (value.isEmpty()) emptyList()
      else value.map { encoder.encode(schema.elementType, it) }
   }
}

/**
 * PassThroughListEncoder is an implementation of the [Encoder] interface specifically designed for encoding
 * a list of values ([List<Any?>]) without any transformation. It is primarily used for cases where the input
 * list should be passed through as-is and no additional encoding logic is required.
 *
 * This encoder ensures the schema type is [Schema.Type.ARRAY]. If the schema type does not match, an
 * exception is thrown.
 */
object PassThroughListEncoder : Encoder<List<Any?>> {
   override fun encode(schema: Schema, value: List<Any?>): List<Any?> {
      require(schema.type == Schema.Type.ARRAY)
      return value
   }
}

/**
 * An [Encoder] for Sets of [T] that returns a list and encodes each element using a delegated [encoder].
 */
class SetEncoder<T>(private val encoder: Encoder<T>) : Encoder<Set<T>> {
   override fun encode(schema: Schema, value: Set<T>): List<Any?> {
      require(schema.type == Schema.Type.ARRAY)
      return if (value.isEmpty()) emptyList()
      else value.map { encoder.encode(schema.elementType, it) }
   }
}

object PassThroughSetEncoder : Encoder<Set<Any?>> {
   override fun encode(schema: Schema, value: Set<Any?>): List<Any?> {
      require(schema.type == Schema.Type.ARRAY)
      return value.toList()
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
