package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.decoders.StringDecoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericArray

/**
 * An [Encoder] for Arrays of [T] that encodes into an Avro [GenericArray].
 */
class ArrayEncoder<T>(private val encoder: Encoder<T>) : Encoder<Array<T>> {
   override fun encode(schema: Schema): (Array<T>) -> Any? {
      require(schema.type == Schema.Type.ARRAY)
      val elements = encoder.encode(schema.elementType)
      return { value ->
         if (value.isEmpty()) emptyList()
         else value.map { elements.invoke(it) }
      }
   }
}

/**
 * An [Encoder] for Lists of [T] that encodes into an Avro [GenericArray].
 */
class ListEncoder<T>(private val encoder: Encoder<T>) : Encoder<List<T>> {
   override fun encode(schema: Schema): (List<T>) -> Any? {
      require(schema.type == Schema.Type.ARRAY)
      val elements = encoder.encode(schema.elementType)
      return { value ->
         if (value.isEmpty()) emptyList()
         else value.map { elements.invoke(it) }
      }
   }
}

/**
 * An [Encoder] for Sets of [T] that encodes into an Avro [GenericArray].
 */
class SetEncoder<T>(private val encoder: Encoder<T>) : Encoder<Set<T>> {
   override fun encode(schema: Schema): (Set<T>) -> Any? {
      require(schema.type == Schema.Type.ARRAY)
      val elements = encoder.encode(schema.elementType)
      return { value ->
         if (value.isEmpty()) emptyList()
         else value.map { elements.invoke(it) }
      }
   }
}

class MapEncoder<T>(
   private val keyEncoder: Encoder<String>,
   private val valueEncoder: Encoder<T>
) : Encoder<Map<String, T>> {
   override fun encode(schema: Schema): (Map<String, T>) -> Any? {
      require(schema.type == Schema.Type.MAP)
      val keys = keyEncoder.encode(StringDecoder.STRING_SCHEMA)
      val values = valueEncoder.encode(schema.valueType)
      return { value ->
         if (value.isEmpty()) emptyMap()
         else value.map { (key, value) -> keys.invoke(key) to values.invoke(value) }.toMap()
      }
   }
}
