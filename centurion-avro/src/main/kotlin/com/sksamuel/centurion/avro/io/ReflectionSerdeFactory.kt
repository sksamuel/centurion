package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.decoders.SpecificRecordDecoder
import com.sksamuel.centurion.avro.encoders.SpecificRecordEncoder
import com.sksamuel.centurion.avro.generation.ReflectionSchemaBuilder
import kotlin.reflect.KClass

/**
 * A [ReflectionSerdeFactory] will create a [Serde] for any given type using reflection based builders.
 *
 * This instance is thread safe.
 */
object ReflectionSerdeFactory {

   /**
    * Creates a [Serde] reflectively from the given [kclass] using a [ReflectionSchemaBuilder].
    */
   fun <T : Any> create(
      kclass: KClass<T>,
      options: SerdeOptions = SerdeOptions()
   ): Serde<T> {
      val schema = ReflectionSchemaBuilder(true).schema(kclass)
      val encoder = SpecificRecordEncoder(kclass)
      val decoder: SpecificRecordDecoder<T> = SpecificRecordDecoder(kclass)
      return Serde(schema, encoder, decoder, options)
   }

   /**
    * Creates a [Serde] reflectively from the given type parameter [T] using a [ReflectionSchemaBuilder].
    */
   inline fun <reified T : Any> create(options: SerdeOptions = SerdeOptions()): Serde<T> {
      return create(T::class, options)
   }
}
