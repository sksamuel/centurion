package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

class SpecificRecordDecoder<T : Any>(
   private val kclass: KClass<T>,
) : Decoder<T> {

   private val decoders = ConcurrentHashMap<String, List<Decoding>>()

   init {
      require(kclass.isData) { "SpecificRecordDecoder only support data class: was $kclass" }
   }

   companion object {
      inline operator fun <reified T : Any> invoke() = SpecificRecordDecoder(T::class)
   }

   private val constructor = kclass.primaryConstructor ?: error("No primary constructor")

   override fun decode(schema: Schema, value: Any?): T {
      val record = value as GenericRecord
      val decoders = decoders.getOrPut(value::class.java.name) { buildDecodings(schema, value::class) }
      val args = decoders.map { (pos, decoder, schema) ->
         val value = record.get(pos)
         decoder.decode(schema, value)
      }
      return constructor.call(*args.toTypedArray<Any?>())
   }

   private fun buildDecodings(schema: Schema, klass: KClass<out Any>): List<Decoding> {
      return constructor.parameters.map { param ->
         val field = schema.getField(param.name)
         val decoder = Decoder.decoderFor(param.type)
         Decoding(field.pos(), decoder, field.schema())
      }
   }

   private data class Decoding(val pos: Int, val decode: Decoder<*>, val schema: Schema)
}
