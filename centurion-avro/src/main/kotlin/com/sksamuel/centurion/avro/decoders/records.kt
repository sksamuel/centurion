package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

class ReflectionRecordDecoder<T : Any>(private val kclass: KClass<T>) : Decoder<T> {

   companion object {
      inline operator fun <reified T : Any> invoke() = ReflectionRecordDecoder(T::class)
   }

   override fun decode(schema: Schema, value: Any?): T {
      require(schema.type == Schema.Type.RECORD)
      require(kclass.isData) { "Decoders only support data class: was $kclass" }
      require(value is GenericRecord) { "ReflectionRecordDecoder only supports GenericRecords: was $value" }
      val args = kclass.primaryConstructor!!.parameters.map { param ->
         val field = schema.getField(param.name)
         val arg = value.get(param.name)
         val decoder = Decoder.decoderFor(param.type)
         decoder.decode(field.schema(), arg)
      }
      require(args.size == kclass.primaryConstructor!!.parameters.size)
      return kclass.primaryConstructor!!.call(*args.toTypedArray())
   }
}
