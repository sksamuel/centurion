package com.sksamuel.centurion.avro.decoders

import com.sksamuel.centurion.avro.schemas.ReflectionSchemaBuilder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

/**
 * A [Decoder] that returns a Kotlin data class [T] for a given [org.apache.avro.generic.GenericRecord],
 * using reflection to access the fields of the data class.
 *
 * The [ReflectionRecordDecoder] will cache the reflective calls upon creation.
 * This encoder requires a small overhead in CPU time to build the reflection calls,
 * verus programmatically generated decoders of around 15-20%. To benefit from the cached encodings,
 * ensure that you create a reflection-based decoder once and re-use it throughout your project.
 *
 * Instances of this class are thread-safe.
 */
class ReflectionRecordDecoder<T : Any>(
   schema: Schema,
   private val kclass: KClass<T>
) : Decoder<T> {

   init {
      require(kclass.isData) { "ReflectionRecordDecoder can only be used with data classes: was $kclass" }
   }

   private val constructor = kclass.primaryConstructor ?: error("No primary constructor for type $kclass")

   companion object {

      /**
       * Creates a [ReflectionRecordDecoder] for the given [schema] and type [T].
       */
      inline operator fun <reified T : Any> invoke(schema: Schema): ReflectionRecordDecoder<T> {
         return ReflectionRecordDecoder(schema, T::class)
      }

      /**
       * Creates a [ReflectionRecordDecoder] for the given type [T].
       * This will use the [ReflectionSchemaBuilder] to generate the Avro schema for the type [T].
       */
      inline operator fun <reified T : Any> invoke(): ReflectionRecordDecoder<T> {
         val schema = ReflectionSchemaBuilder().schema(T::class)
         return ReflectionRecordDecoder(schema, T::class)
      }

      /**
       * Creates a [ReflectionRecordDecoder] for the given type [T].
       * This will use the [ReflectionSchemaBuilder] to generate the Avro schema for the type [T].
       */
      operator fun <T : Any> invoke(kclass: KClass<T>): ReflectionRecordDecoder<T> {
         val schema = ReflectionSchemaBuilder().schema(kclass)
         return ReflectionRecordDecoder(schema, kclass)
      }
   }

   private val decoders: Array<Decoding> = buildDecodings(schema)

   override fun decode(schema: Schema, value: Any?): T {
      val record = value as GenericRecord
      val decoders = this.decoders
      val args = arrayOfNulls<Any?>(decoders.size)
      var i = 0
      while (i < decoders.size) {
         val decoding = decoders[i]
         args[i] = decoding.decoder.decode(decoding.schema, record.get(decoding.pos))
         i++
      }
      return constructor.call(*args)
   }

   private fun buildDecodings(schema: Schema): Array<Decoding> {
      val params = constructor.parameters
      return Array(params.size) { i ->
         val param = params[i]
         val avroField = schema.getField(param.name)
            ?: error("Could not find field ${param.name} in Avro schema")
         val decoder = Decoder.decoderFor(param.type, avroField.schema())
         Decoding(avroField.pos(), decoder, avroField.schema())
      }
   }

   private class Decoding(val pos: Int, val decoder: Decoder<*>, val schema: Schema)
}
