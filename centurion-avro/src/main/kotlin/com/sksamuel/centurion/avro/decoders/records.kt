package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

class SpecificRecordDecoder<T : Any>(
   private val kclass: KClass<T>,
   private val schema: Schema,
) : Decoder<T> {

   init {
      require(schema.type == Schema.Type.RECORD)
      require(kclass.isData) { "Decoders only support data class: was $kclass" }
   }

   companion object {
      inline operator fun <reified T : Any> invoke(schema: Schema) = SpecificRecordDecoder(T::class, schema)
   }

   private val constructor = kclass.primaryConstructor ?: error("No primary constructor")

   private val members = constructor.parameters.map { param ->
      val field = schema.getField(param.name)
      val decoder = Decoder.decoderFor(param.type)
      Triple(param.name, field, decoder)
   }

   override fun decode(schema: Schema, value: Any?): T {
      require(value is GenericRecord) { "ReflectionRecordDecoder only supports GenericRecords: was $value" }
      val args = members.map { (name, field, decoder) ->
         val arg = value.get(name)
         decoder.decode(field.schema(), arg)
      }
      return constructor.call(*args.toTypedArray())
   }
}
