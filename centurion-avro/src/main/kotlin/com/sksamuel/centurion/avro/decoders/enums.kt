package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericEnumSymbol
import kotlin.reflect.KClass

class EnumDecoder<T : Enum<T>>(kclass: KClass<T>) : Decoder<Enum<T>> {

   private val j: Class<T> = kclass.java

   init {
      require(kclass.java.isEnum)
   }

   companion object {
      inline operator fun <reified T : Enum<T>> invoke() = EnumDecoder(T::class)
   }

   override fun decode(schema: Schema, value: Any?): Enum<T> {
      require(schema.type == Schema.Type.ENUM)
      return when (value) {
         is GenericEnumSymbol<*> -> java.lang.Enum.valueOf(j, value.toString())
         else -> error("Unsupported enum type $value")
      }
   }
}
