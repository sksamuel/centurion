package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

class ReflectionRecordDecoder<T : Any>(
   private val kclass: KClass<T>,
) : Decoder<T> {

   init {
      require(kclass.isData) { "SpecificRecordDecoder only support data classes: was $kclass" }
   }

   companion object {
      inline operator fun <reified T : Any> invoke() = ReflectionRecordDecoder(T::class)
   }

   private val constructor = kclass.primaryConstructor ?: error("No primary constructor")
   private val decoders = ConcurrentHashMap<String, (GenericRecord) -> T>()

   override fun decode(schema: Schema, value: Any?): T {
      val record = value as GenericRecord
      val decodeFn = decoders.getOrPut(value::class.java.name) { decoderFn(schema) }
      return decodeFn(record)
   }

   private fun decoderFn(schema: Schema): (GenericRecord) -> T {
      val decoders = constructor.parameters.map { param ->
         val avroField = schema.getField(param.name)
         val decoder = Decoder.decoderFor(param.type)
         Decoding(avroField.pos(), decoder, avroField.schema())
      }

      return { record ->
         val args = decoders.map { (pos, decoder, schema) ->
            val value = record.get(pos)
            decoder.decode(schema, value)
         }
         constructor.call(*args.toTypedArray())
      }
   }

   private data class Decoding(val pos: Int, val decode: Decoder<*>, val schema: Schema)
}
