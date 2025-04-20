package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.util.Utf8
import kotlin.reflect.KClass

class EnumDecoder<T : Enum<T>>(kclass: KClass<T>) : Decoder<T> {

   private val j: Class<T> = kclass.java

   init {
      require(kclass.java.isEnum)
   }

   companion object {
      inline operator fun <reified T : Enum<T>> invoke() = EnumDecoder(T::class)
   }

   override fun decode(schema: Schema, value: Any?): T {
      require(schema.type == Schema.Type.ENUM)
      val map = j.enumConstants.associateBy { it.name }
      val symbol = when (value) {
         is GenericEnumSymbol<*> -> value.toString()
         is String -> value
         is Utf8 -> value.toString()
         else -> error("Unsupported enum container $value")
      }
      return map[symbol] ?: error("Unknown symbol $value")
   }
}
