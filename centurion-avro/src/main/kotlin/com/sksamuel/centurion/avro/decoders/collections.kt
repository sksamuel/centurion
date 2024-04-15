package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class ListDecoder<T>(private val decoder: Decoder<T>) : Decoder<List<T>> {
   override fun decode(schema: Schema, value: Any?): List<T> {
      require(schema.type == Schema.Type.ARRAY)
      return when (value) {
         is GenericData.Array<*> -> value.map { decoder.decode(schema.elementType, it) }
         is List<*> -> value.map { decoder.decode(schema.elementType, it) }
         is Array<*> -> value.map { decoder.decode(schema.elementType, it) }
         else -> error("Unsupported list type $value")
      }
   }
}

class SetDecoder<T>(private val decoder: Decoder<T>) : Decoder<Set<T>> {
   override fun decode(schema: Schema, value: Any?): Set<T> {
      require(schema.type == Schema.Type.ARRAY)
      return when (value) {
         is GenericData.Array<*> -> value.map { decoder.decode(schema.elementType, it) }.toSet()
         is List<*> -> value.map { decoder.decode(schema.elementType, it) }.toSet()
         is Array<*> -> value.map { decoder.decode(schema.elementType, it) }.toSet()
         else -> error("Unsupported set type $value")
      }
   }
}

class MapDecoder<T>(private val decoder: Decoder<T>) : Decoder<Map<String, T>> {
   override fun decode(schema: Schema, value: Any?): Map<String, T> {
      require(schema.type == Schema.Type.MAP)
      return when (value) {
         is Map<*, *> -> value.map { (k, v) -> Pair(k as String, decoder.decode(schema.valueType, v)) }.toMap()
         else -> error("Unsupported map type $value")
      }
   }
}
