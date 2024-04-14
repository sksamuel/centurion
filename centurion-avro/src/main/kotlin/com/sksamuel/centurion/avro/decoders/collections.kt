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
         else -> error("Unsupported list type $value")
      }
   }
}
