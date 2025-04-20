package com.sksamuel.centurion.avro.io.serde

import com.sksamuel.centurion.avro.decoders.ReflectionRecordDecoder
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.encoders.SpecificReflectionRecordEncoder
import com.sksamuel.centurion.avro.io.Format
import com.sksamuel.centurion.avro.schemas.ReflectionSchemaBuilder
import kotlin.reflect.KClass

/**
 * A [ReflectionSerdeFactory] will create a [Serde] for a given type using reflection based builders.
 *
 * The underlying reflection based builders will cache reflection information for the given type
 * so the serde should be reused for performance.
 *
 * This instance is thread safe.
 */
object ReflectionSerdeFactory : SerdeFactory() {

   /**
    * Creates a [Serde] reflectively from the given [kclass] using a [ReflectionSchemaBuilder],
    * [ReflectionRecordEncoder] and [ReflectionRecordDecoder].
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
      val decoder = ReflectionRecordDecoder<T>(kclass)
      return when (format) {
         Format.Binary -> BinarySerde(schema, encoder, decoder, options)
         Format.Data -> DataSerde(schema, encoder, decoder, options)
         Format.Json -> JsonSerde(schema, encoder, decoder, options)
      }
   }
}
