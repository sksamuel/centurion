package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericData

/**
 * An [Encoder] for Arrays of [T] that encodes into an Avro [GenericArray].
 */
class GenericArrayArrayEncoder<T>(private val encoder: Encoder<T>) : Encoder<Array<T>> {
   override fun encode(schema: Schema, value: Array<T>): Any {
      require(schema.type == Schema.Type.ARRAY)
      val elements = value.map { encoder.encode(schema.elementType, it) }
      return GenericData.Array(schema, elements)
   }
}

/**
 * An [Encoder] for Lists of [T] that encodes into an Avro [GenericArray].
 */
class GenericArrayListEncoder<T>(private val encoder: Encoder<T>) : Encoder<List<T>> {
   override fun encode(schema: Schema, value: List<T>): Any {
      require(schema.type == Schema.Type.ARRAY)
      val elements = value.map { encoder.encode(schema.elementType, it) }
      return GenericData.Array(schema, elements)
   }
}

/**
 * An [Encoder] for Sets of [T] that encodes into an Avro [GenericArray].
 */
class GenericArraySetEncoder<T>(private val encoder: Encoder<T>) : Encoder<Set<T>> {
   override fun encode(schema: Schema, value: Set<T>): GenericArray<Any> {
      require(schema.type == Schema.Type.ARRAY)
      val elements = value.map { encoder.encode(schema.elementType, it) }
      return GenericData.Array(schema, elements)
   }
}

class MapEncoder<T>(private val encoder: Encoder<T>) : Encoder<Map<String, T>> {
   override fun encode(schema: Schema, value: Map<String, T>): Map<String, Any?> {
      require(schema.type == Schema.Type.MAP)
      return value.mapValues { encoder.encode(schema.valueType, it.value) }
   }
}
