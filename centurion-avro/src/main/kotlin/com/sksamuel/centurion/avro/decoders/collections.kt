package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class IntArrayDecoder(private val decoder: Decoder<Int>) : Decoder<IntArray> {
   override fun decode(schema: Schema, value: Any?): IntArray {
      require(schema.type == Schema.Type.ARRAY)
      return when (value) {
         is GenericData.Array<*> -> value.map { decoder.decode(schema.elementType, it) }.toTypedArray().toIntArray()
         is List<*> -> value.map { decoder.decode(schema.elementType, it) }.toTypedArray().toIntArray()
         is Array<*> -> value.map { decoder.decode(schema.elementType, it) }.toTypedArray().toIntArray()
         else -> error("Unsupported list type $value")
      }
   }
}

class LongArrayDecoder(private val decoder: Decoder<Long>) : Decoder<LongArray> {
   override fun decode(schema: Schema, value: Any?): LongArray {
      require(schema.type == Schema.Type.ARRAY)
      return when (value) {
         is GenericData.Array<*> -> value.map { decoder.decode(schema.elementType, it) }.toTypedArray().toLongArray()
         is List<*> -> value.map { decoder.decode(schema.elementType, it) }.toTypedArray().toLongArray()
         is Array<*> -> value.map { decoder.decode(schema.elementType, it) }.toTypedArray().toLongArray()
         else -> error("Unsupported list type $value")
      }
   }
}

@Suppress("UNCHECKED_CAST")
object LongListDecoder : Decoder<List<Long>> {
   override fun decode(schema: Schema, value: Any?): List<Long> {
      return when (value) {
         is List<*> -> value as List<Long>
         is Collection<*> -> value.toList() as List<Long>
         is Array<*> -> value.toList() as List<Long>
         else -> error("Unsupported list type $value")
      }
   }
}

@Suppress("UNCHECKED_CAST")
object IntListDecoder : Decoder<List<Int>> {
   override fun decode(schema: Schema, value: Any?): List<Int> {
      return when (value) {
         is List<*> -> value as List<Int>
         is Collection<*> -> value.toList() as List<Int>
         is Array<*> -> value.toList() as List<Int>
         else -> error("Unsupported list type $value")
      }
   }
}

class ListDecoder<T>(private val decoder: Decoder<T>) : Decoder<List<T>> {
   override fun decode(schema: Schema, value: Any?): List<T> {
      val elementType = schema.elementType
      return when (value) {
         is Collection<*> -> value.map { decoder.decode(elementType, it) }
         is Array<*> -> value.map { decoder.decode(elementType, it) }
         else -> error("Unsupported list type $value")
      }
   }
}

@Suppress("UNCHECKED_CAST")
object LongSetDecoder : Decoder<Set<Long>> {
   override fun decode(schema: Schema, value: Any?): Set<Long> {
      return when (value) {
         is List<*> -> (value as List<Long>).toSet()
         is Collection<*> -> (value.toList() as List<Long>).toSet()
         is Array<*> -> (value.toList() as List<Long>).toSet()
         else -> error("Unsupported list type $value")
      }
   }
}

@Suppress("UNCHECKED_CAST")
object IntSetDecoder : Decoder<Set<Int>> {
   override fun decode(schema: Schema, value: Any?): Set<Int> {
      return when (value) {
         is List<*> -> (value as List<Int>).toSet()
         is Collection<*> -> (value.toList() as List<Int>).toSet()
         is Array<*> -> (value.toList() as List<Int>).toSet()
         else -> error("Unsupported list type $value")
      }
   }
}

class SetDecoder<T>(private val decoder: Decoder<T>) : Decoder<Set<T>> {
   override fun decode(schema: Schema, value: Any?): Set<T> {
      require(schema.type == Schema.Type.ARRAY)
      return when (value) {
         is Collection<*> -> value.map { decoder.decode(schema.elementType, it) }.toSet()
         is Array<*> -> value.map { decoder.decode(schema.elementType, it) }.toSet()
         else -> error("Unsupported set type $value")
      }
   }
}

class MapDecoder<T>(private val decoder: Decoder<T>) : Decoder<Map<String, T>> {

   private val STRING_SCHEMA: Schema = Schema.create(Schema.Type.STRING)

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
