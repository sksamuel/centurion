package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

class ReflectionRecordDecoder<T : Any> : Decoder<T> {

   override fun decode(schema: Schema, value: Any?): T {
      require(schema.type == Schema.Type.RECORD)
      require(value is GenericRecord) { "ReflectionRecordDecoder only supports GenericRecords: was $value" }
      val kclass = value::class
      require(kclass.isData) { "Decoders only support data class: was $kclass" }
      val args = kclass.primaryConstructor!!.parameters.map { param ->
         val field = schema.getField(param.name)
         val arg = value.get(param.name)
         val decoder = Decoder.decoderFor(param.type)
         decoder.decode(field.schema(), arg)
      }
      require(args.size == kclass.primaryConstructor!!.parameters.size)
      return kclass.primaryConstructor!!.call(*args.toTypedArray()) as T
   }
}

class SpecificRecordDecoder<T : Any>(
   private val kclass: KClass<T>,
   private val schema: Schema,
) : Decoder<T> {

   init {
      require(schema.type == Schema.Type.RECORD)
      require(kclass.isData) { "Decoders only support data class: was $kclass" }
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
