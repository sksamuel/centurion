package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class ListDecoder<T>(private val decoder: Decoder<T>) : Decoder<List<T>> {
   override fun decode(schema: Schema): (Any?) -> List<T> {
      require(schema.type == Schema.Type.ARRAY)
      val decode = decoder.decode(schema.elementType)
      return { value ->
         when (value) {
            is GenericData.Array<*> -> value.map { decode.invoke(it) }
            is List<*> -> value.map { decode.invoke(it) }
            is Array<*> -> value.map { decode.invoke(it) }
            else -> error("Unsupported list type $value")
         }
      }
   }
}

class SetDecoder<T>(private val decoder: Decoder<T>) : Decoder<Set<T>> {
   override fun decode(schema: Schema): (Any?) -> Set<T> {
      require(schema.type == Schema.Type.ARRAY)
      val decode = decoder.decode(schema.elementType)
      return { value ->
         when (value) {
            is GenericData.Array<*> -> value.map { decode.invoke(it) }.toSet()
            is List<*> -> value.map { decode.invoke(it) }.toSet()
            is Array<*> -> value.map { decode.invoke(it) }.toSet()
            else -> error("Unsupported set type $value")
         }
      }
   }
}

class MapDecoder<T>(private val decoder: Decoder<T>) : Decoder<Map<String, T>> {
   override fun decode(schema: Schema): (Any?) -> Map<String, T> {
      require(schema.type == Schema.Type.MAP)
      val keyDecode = StringDecoder.decode(StringDecoder.STRING_SCHEMA)
      val valueDecode = decoder.decode(schema.valueType)
      return { value ->
         when (value) {
            is Map<*, *> -> value.map { (k, v) -> keyDecode.invoke(k) to valueDecode.invoke(v) }.toMap()
            else -> error("Unsupported map type $value")
         }
      }
   }
}
