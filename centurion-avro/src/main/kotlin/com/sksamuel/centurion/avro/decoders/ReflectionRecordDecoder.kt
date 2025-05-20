package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

/**
 * A [Decoder] that returns a Kotlin data class [T] for a given [org.apache.avro.generic.GenericRecord],
 * using reflection to access the fields of the data class.
 *
 * The [ReflectionRecordDecoder] will cache the reflection calls upon first use.
 * This encoder requires a small overhead in CPU time to build the reflection calls,
 * verus programmatically generated decoders of around 15-20%. To benefit from the cached encodings,
 * ensure that you create a reflection-based decoder once and re-use it throughout your project.
 *
 * Instances of this class are thread-safe.
 */
class ReflectionRecordDecoder<T : Any>(private val kclass: KClass<T>) : Decoder<T> {

   init {
      require(kclass.isData) { "ReflectionRecordDecoder can only be used with data classes: was $kclass" }
   }

   private val constructor = kclass.primaryConstructor ?: error("No primary constructor for type $kclass")

   companion object {
      inline operator fun <reified T : Any> invoke(): ReflectionRecordDecoder<T> = ReflectionRecordDecoder(T::class)
   }

   // this isn't thread safe, but worst case is we generate the same encoders more than once
   // in which case we will have a tiny performance hit initially, but the idea is this class is
   // created once and re-used throughout the service's lifetime
   private var decodeFn: ((GenericRecord) -> T)? = null

   override fun decode(schema: Schema, value: Any?): T {
      val record = value as GenericRecord
      if (decodeFn == null)
         decodeFn = decoderFn(schema)
      return decodeFn!!(record)
   }

   private fun decoderFn(schema: Schema): (GenericRecord) -> T {

      val decoders = constructor.parameters.map { param ->
         val avroField = schema.getField(param.name)
         val decoder = Decoder.decoderFor(param.type, schema.getProp(GenericData.STRING_PROP))
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
