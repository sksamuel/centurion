package com.sksamuel.centurion.avro.io.serde

import com.sksamuel.centurion.avro.decoders.SpecificRecordDecoder
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.encoders.SpecificReflectionRecordEncoder
import com.sksamuel.centurion.avro.generation.ReflectionSchemaBuilder
import com.sksamuel.centurion.avro.io.Format
import kotlin.reflect.KClass

/**
 * A [ReflectionSerdeFactory] will create a [Serde] for a given type using reflection based builders.
 *
 * This instance is thread safe.
 */
object ReflectionSerdeFactory : SerdeFactory() {

   /**
    * Creates a [Serde] reflectively from the given [kclass] using a [ReflectionSchemaBuilder],
    * [ReflectionRecordEncoder] and [SpecificRecordDecoder].
    *
    * @param format specify the type of output.
    */
   override fun <T : Any> create(
      kclass: KClass<T>,
      format: Format,
      options: SerdeOptions
   ): Serde<T> {
      val schema = ReflectionSchemaBuilder(true).schema(kclass)
      val encoder = SpecificReflectionRecordEncoder<T>()
      val decoder = SpecificRecordDecoder(kclass)
      return when (format) {
         Format.Binary -> BinarySerde(schema, encoder, decoder, options)
         Format.Data -> TODO()
         Format.Json -> TODO()
      }
   }
}
