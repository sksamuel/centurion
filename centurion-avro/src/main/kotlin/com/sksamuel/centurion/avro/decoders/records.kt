package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

class SpecificRecordDecoder<T : Any>(
   private val kclass: KClass<T>,
) : Decoder<T> {

   init {
      require(kclass.isData) { "SpecificRecordDecoder only support data classes: was $kclass" }
   }

   companion object {
      inline operator fun <reified T : Any> invoke() = SpecificRecordDecoder(T::class)
   }

   private val constructor = kclass.primaryConstructor ?: error("No primary constructor")
   private val decoders = ConcurrentHashMap<String, List<Decoding>>()

   override fun decode(schema: Schema, value: Any?): T {
      val record = value as GenericRecord
      val decoders = decoders.getOrPut(value::class.java.name) { buildDecodings(schema) }
      val args = decoders.map { (pos, decoder, schema) ->
         val value = record.get(pos)
         decoder.decode(schema, value)
      }
      return constructor.call(*args.toTypedArray<Any?>())
   }

   private fun buildDecodings(schema: Schema): List<Decoding> {
      return constructor.parameters.map { param ->
         val avroField = schema.getField(param.name)
         val decoder = Decoder.decoderFor(param.type)
         Decoding(avroField.pos(), decoder, avroField.schema())
      }
   }

   private data class Decoding(val pos: Int, val decode: Decoder<*>, val schema: Schema)
}
