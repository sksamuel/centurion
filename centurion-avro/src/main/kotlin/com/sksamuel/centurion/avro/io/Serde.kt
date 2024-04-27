package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.decoders.SpecificRecordDecoder
import com.sksamuel.centurion.avro.encoders.RecordDecoder
import com.sksamuel.centurion.avro.encoders.RecordEncoder
import com.sksamuel.centurion.avro.encoders.SpecificRecordEncoder
import com.sksamuel.centurion.avro.generation.ReflectionSchemaBuilder
import org.apache.avro.Schema
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayInputStream
import kotlin.reflect.KClass

/**
 * A [Serde] provides an easy way to convert between data classes and avro encoded bytes
 * using reflection based encoders and decoders.
 *
 * This class is thread safe.
 */
class Serde<T : Any>(schema: Schema, kclass: KClass<T>) {

   init {
      require(kclass.isData)
   }

   companion object {

      /**
       * Creates a [Schema] reflectively from the given [kclass] using a [ReflectionSchemaBuilder].
       */
      operator fun <T : Any> invoke(kclass: KClass<T>): Serde<T> {
         val schema = ReflectionSchemaBuilder(true).schema(kclass)
         return Serde(schema, kclass)
      }

      /**
       * Creates a [Schema] reflectively from the given type parameter [T] using a [ReflectionSchemaBuilder].
       */
      inline operator fun <reified T : Any> invoke(): Serde<T> {
         val schema = ReflectionSchemaBuilder(true).schema(T::class)
         return Serde(schema, T::class)
      }
   }

   private val encoder = RecordEncoder(schema, SpecificRecordEncoder(kclass, schema))
   private val decoder = RecordDecoder(SpecificRecordDecoder(kclass, schema))
   private val writerFactory = BinaryWriterFactory(schema, EncoderFactory())
   private val readerFactory = BinaryReaderFactory(schema)

   fun serialize(obj: T): ByteArray = writerFactory.write(encoder.encode(obj))
   fun deserialize(bytes: ByteArray): T = decoder.decode((readerFactory.read(ByteArrayInputStream(bytes))))
}
