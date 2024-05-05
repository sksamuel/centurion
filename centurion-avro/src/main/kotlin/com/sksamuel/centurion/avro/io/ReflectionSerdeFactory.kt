package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.decoders.SpecificRecordDecoder
import com.sksamuel.centurion.avro.encoders.SpecificRecordEncoder
import com.sksamuel.centurion.avro.generation.ReflectionSchemaBuilder
import kotlin.reflect.KClass

/**
 * A [ReflectionSerdeFactory] will create a [SpecificSerde] for a given type using
 * reflection based builders.
 *
 * This instance is thread safe.
 */
object ReflectionSerdeFactory {

   /**
    * Creates a [SpecificSerde] reflectively from the given [kclass] using a [ReflectionSchemaBuilder],
    * [SpecificRecordEncoder] and [SpecificRecordDecoder].
    */
   fun <T : Any> create(
      kclass: KClass<T>,
      options: SerdeOptions = SerdeOptions()
   ): SpecificSerde<T> {
      val schema = ReflectionSchemaBuilder(true).schema(kclass)
      val encoder = SpecificRecordEncoder(kclass)
      val decoder = SpecificRecordDecoder(kclass)
      return SpecificSerde(schema, encoder, decoder, options)
   }

   /**
    * Creates a [SpecificSerde] reflectively from the given type parameter [T] using a [ReflectionSchemaBuilder],
    * [SpecificRecordEncoder] and [SpecificRecordDecoder].
    */
   inline fun <reified T : Any> create(options: SerdeOptions = SerdeOptions()): SpecificSerde<T> {
      return create(T::class, options)
   }
}
