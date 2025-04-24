package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

/**
 * A [Decoder] that returns a Kotlin data class [T] for a given [org.apache.avro.generic.GenericRecord],
 * using reflection to access the fields of the data class.
 *
 * The [ReflectionRecordDecoder] will cache the reflection calls upon first use.
 * This encoder requires a small overhead in CPU time to build the reflection calls,
 * verus programmatically generated decoders of around 10-15%. To benefit from the cached encodings,
 * ensure that you create a reflection based decoder once and re-use it throughout your project.
 *
 * Instances of this class are thread safe.
 */class ReflectionRecordDecoder<T : Any>(private val kclass: KClass<T>) : Decoder<T> {

   init {
      require(kclass.isData) { "ReflectionRecordDecoder can only be used with data classes: was $kclass" }
   }

   companion object {
      inline operator fun <reified T : Any> invoke(): ReflectionRecordDecoder<T> = ReflectionRecordDecoder(T::class)
   }

   private val decoders = ConcurrentHashMap<String, (GenericRecord) -> T>()

   override fun decode(schema: Schema, value: Any?): T {
      val record = value as GenericRecord
      val decodeFn = decoders.getOrPut(value::class.java.name) { decoderFn(schema) }
      return decodeFn(record)
   }

   private fun decoderFn(schema: Schema): (GenericRecord) -> T {

      val constructor = kclass.primaryConstructor ?: error("No primary constructor for type $kclass")
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
