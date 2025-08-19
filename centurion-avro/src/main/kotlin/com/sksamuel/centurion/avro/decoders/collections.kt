package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema

class IntArrayDecoder(private val decoder: Decoder<Int>) : Decoder<IntArray> {
   override fun decode(schema: Schema, value: Any?): IntArray {
      require(schema.type == Schema.Type.ARRAY)
      return when (value) {
         // put array first as avro encodes as generic array mostly
         is Array<*> -> value.map { decoder.decode(schema.elementType, it) }.toTypedArray().toIntArray()
         is List<*> -> value.map { decoder.decode(schema.elementType, it) }.toTypedArray().toIntArray()
         else -> error("Unsupported list type $value")
      }
   }
}

class LongArrayDecoder(private val decoder: Decoder<Long>) : Decoder<LongArray> {
   override fun decode(schema: Schema, value: Any?): LongArray {
      require(schema.type == Schema.Type.ARRAY)
      return when (value) {
         // put array first as avro encodes as generic array mostly
         is Array<*> -> value.map { decoder.decode(schema.elementType, it) }.toTypedArray().toLongArray()
         is List<*> -> value.map { decoder.decode(schema.elementType, it) }.toTypedArray().toLongArray()
         else -> error("Unsupported list type $value")
      }
   }
}

object PassthroughListDecoder : Decoder<List<Any?>> {
   override fun decode(schema: Schema, value: Any?): List<Any?> {
      return when (value) {
         // put array first as avro encodes as generic array mostly
         is Array<*> -> value.asList()
         is List<*> -> value
         is Collection<*> -> value.toList()
         else -> error("Unsupported list type $value")
      }
   }
}

class ListDecoder<T>(private val decoder: Decoder<T>) : Decoder<List<T>> {
   override fun decode(schema: Schema, value: Any?): List<T> {
      val elementType = schema.elementType
      return when (value) {
         // put array first as avro encodes as generic array mostly
         is Array<*> -> value.map { decoder.decode(elementType, it) }
         is Collection<*> -> value.map { decoder.decode(elementType, it) }
         else -> error("Unsupported list type $value")
      }
   }
}

object PassthroughSetDecoder : Decoder<Set<Any?>> {
   override fun decode(schema: Schema, value: Any?): Set<Any?> {
      return when (value) {
         // put array first as avro encodes as generic array mostly
         is Array<*> -> value.toSet()
         is Set<*> -> value
         is Collection<*> -> value.toSet()
         else -> error("Unsupported list type $value")
      }
   }
}

class SetDecoder<T>(private val decoder: Decoder<T>) : Decoder<Set<T>> {
   override fun decode(schema: Schema, value: Any?): Set<T> {
      require(schema.type == Schema.Type.ARRAY)
      return when (value) {
         // put array first as avro encodes as generic array mostly
         is Array<*> -> value.map { decoder.decode(schema.elementType, it) }.toSet()
         is Collection<*> -> value.map { decoder.decode(schema.elementType, it) }.toSet()
         else -> error("Unsupported set type $value")
      }
   }
}

class MapDecoder<T>(private val decoder: Decoder<T>) : Decoder<Map<String, T>> {

   companion object {
      private val STRING_SCHEMA: Schema = Schema.create(Schema.Type.STRING)
   }

   override fun decode(schema: Schema, value: Any?): Map<String, T> {
      require(schema.type == Schema.Type.MAP)
      return when (value) {
         is Map<*, *> -> {
            value.map { (k, v) ->
               StringDecoder.decode(STRING_SCHEMA, k) to decoder.decode(schema.valueType, v)
            }.toMap()
         }

         else -> {
            error("Unsupported map type $value")
         }
      }
   }
}
