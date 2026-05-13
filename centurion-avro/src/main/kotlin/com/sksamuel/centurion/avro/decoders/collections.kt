package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema

class IntArrayDecoder(private val decoder: Decoder<Int>) : Decoder<IntArray> {
   override fun decode(schema: Schema, value: Any?): IntArray {
      val elementType = schema.elementType
      return when (value) {
         // put list first as avro encodes as GenericArray mostly
         is List<*> -> IntArray(value.size) { i -> decoder.decode(elementType, value[i]) }
         is Array<*> -> IntArray(value.size) { i -> decoder.decode(elementType, value[i]) }
         else -> error("Unsupported list type $value")
      }
   }
}

class LongArrayDecoder(private val decoder: Decoder<Long>) : Decoder<LongArray> {
   override fun decode(schema: Schema, value: Any?): LongArray {
      val elementType = schema.elementType
      return when (value) {
         // put list first as avro encodes as GenericArray mostly
         is List<*> -> LongArray(value.size) { i -> decoder.decode(elementType, value[i]) }
         is Array<*> -> LongArray(value.size) { i -> decoder.decode(elementType, value[i]) }
         else -> error("Unsupported list type $value")
      }
   }
}

object PassthroughListDecoder : Decoder<List<Any?>> {
   override fun decode(schema: Schema, value: Any?): List<Any?> {
      return when (value) {
         // put list first as avro encodes as GenericArray mostly
         is List<*> -> value
         is Array<*> -> value.asList()
         is Collection<*> -> value.toList()
         else -> error("Unsupported list type $value")
      }
   }
}

class ListDecoder<T>(private val decoder: Decoder<T>) : Decoder<List<T>> {
   override fun decode(schema: Schema, value: Any?): List<T> {
      val elementType = schema.elementType
      return when (value) {
         // put list first as avro encodes as GenericArray mostly
         is List<*> -> {
            val size = value.size
            val result = ArrayList<T>(size)
            if (value is RandomAccess) {
               for (i in 0 until size) {
                  result.add(decoder.decode(elementType, value[i]))
               }
            } else {
               for (element in value) {
                  result.add(decoder.decode(elementType, element))
               }
            }
            result
         }
         is Collection<*> -> {
            val result = ArrayList<T>(value.size)
            for (element in value) {
               result.add(decoder.decode(elementType, element))
            }
            result
         }
         is Array<*> -> {
            val result = ArrayList<T>(value.size)
            for (i in value.indices) {
               result.add(decoder.decode(elementType, value[i]))
            }
            result
         }
         else -> error("Unsupported list type $value")
      }
   }
}

object PassthroughSetDecoder : Decoder<Set<Any?>> {
   override fun decode(schema: Schema, value: Any?): Set<Any?> {
      return when (value) {
         // put list first as avro encodes as GenericArray mostly
         is Collection<*> -> value.toSet()
         is Array<*> -> value.toSet()
         else -> error("Unsupported list type $value")
      }
   }
}

class SetDecoder<T>(private val decoder: Decoder<T>) : Decoder<Set<T>> {
   override fun decode(schema: Schema, value: Any?): Set<T> {
      val elementType = schema.elementType
      return when (value) {
         // put list first as avro encodes as GenericArray mostly
         is List<*> -> {
            val size = value.size
            val result = LinkedHashSet<T>(size)
            if (value is RandomAccess) {
               for (i in 0 until size) {
                  result.add(decoder.decode(elementType, value[i]))
               }
            } else {
               for (element in value) {
                  result.add(decoder.decode(elementType, element))
               }
            }
            result
         }
         is Collection<*> -> {
            val result = LinkedHashSet<T>(value.size)
            for (element in value) {
               result.add(decoder.decode(elementType, element))
            }
            result
         }
         is Array<*> -> {
            val result = LinkedHashSet<T>(value.size)
            for (i in value.indices) {
               result.add(decoder.decode(elementType, value[i]))
            }
            result
         }
         else -> error("Unsupported set type $value")
      }
   }
}

class MapDecoder<T>(private val decoder: Decoder<T>) : Decoder<Map<String, T>> {

   companion object {
      private val STRING_SCHEMA: Schema = Schema.create(Schema.Type.STRING)
   }

   override fun decode(schema: Schema, value: Any?): Map<String, T> {
      return when (value) {
         is Map<*, *> -> {
            val valueType = schema.valueType
            val result = LinkedHashMap<String, T>(value.size)
            value.forEach { (k, v) ->
               result[StringDecoder.decode(STRING_SCHEMA, k)] = decoder.decode(valueType, v)
            }
            result
         }
         else -> error("Unsupported map type $value")
      }
   }
}
