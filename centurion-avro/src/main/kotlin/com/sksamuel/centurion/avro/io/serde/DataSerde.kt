package com.sksamuel.centurion.avro.io.serde

import com.sksamuel.centurion.avro.decoders.Decoder
import com.sksamuel.centurion.avro.decoders.ReflectionRecordDecoder
import com.sksamuel.centurion.avro.encoders.Encoder
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.io.DataReader
import com.sksamuel.centurion.avro.io.DataWriter
import com.sksamuel.centurion.avro.schemas.ReflectionSchemaBuilder
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import kotlin.reflect.KClass

/**
 * A [DataSerde] reads and writes in the avro "data" format which includes the schema in the written bytes.
 *
 * This format results in larger sizes than [BinarySerde], as clearly including the schema requires
 * more bytes, but supports schema evolution, as the deserializers can compare the expected schema
 * with the written schema.
 */
class DataSerde<T : Any>(
   private val schema: Schema,
   private val encoder: Encoder<T>,
   private val decoder: Decoder<T>,
   private val codecFactory: CodecFactory?,
) : Serde<T> {

   companion object {

      /**
       * Creates a [DataSerde] for the given type [T] using reflection to generate the schema,
       * encoder and decoder.
       */
      inline operator fun <reified T : Any> invoke(
         encoderFactory: EncoderFactory,
         decoderFactory: DecoderFactory,
         codecFactory: CodecFactory?,
      ): DataSerde<T> {
         return invoke(T::class, encoderFactory, decoderFactory, codecFactory)
      }

      /**
       * Creates a [DataSerde] for the given kclass using reflection to generate the schema,
       * encoder and decoder.
       */
      operator fun <T : Any> invoke(
         kclass: KClass<T>,
         encoderFactory: EncoderFactory,
         decoderFactory: DecoderFactory,
         codecFactory: CodecFactory?,
      ): DataSerde<T> {
         val schema = ReflectionSchemaBuilder(true).schema(kclass)
         val encoder = ReflectionRecordEncoder(schema, kclass)
         val decoder = ReflectionRecordDecoder(schema, kclass)
         return DataSerde(schema, encoder, decoder, codecFactory)
      }
   }

   override fun serialize(obj: T): ByteArray {
      val baos = ByteArrayOutputStream()
      DataWriter(schema, baos, encoder, codecFactory).use { it.write(obj) }
      return baos.toByteArray()
   }

   override fun deserialize(bytes: ByteArray): T {
      return DataReader(bytes, schema, decoder).use { it.next() }
   }
}
