package com.sksamuel.centurion.avro.io.serde

import com.sksamuel.centurion.avro.decoders.Decoder
import com.sksamuel.centurion.avro.decoders.ReflectionRecordDecoder
import com.sksamuel.centurion.avro.encoders.Encoder
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.io.BinaryEncoderPool
import com.sksamuel.centurion.avro.io.BinaryReader
import com.sksamuel.centurion.avro.io.BinaryWriter
import com.sksamuel.centurion.avro.schemas.ReflectionSchemaBuilder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import kotlin.reflect.KClass

/**
 * A [BinarySerde] reads and writes in the avro "binary" format which does not include the schema
 * in the written bytes.
 *
 * This results in a smaller payload, similar to protobuf, but requires that the schema is provided at
 * deserialization time. This format is especially effective when the consumers and producers both know the
 * schema that was used, for instance, in versioned endpoints, or when the same application is used to read
 * and write the data.
 */
class BinarySerde<T : Any>(
   private val schema: Schema,
   private val encoder: Encoder<T>,
   private val decoder: Decoder<T>,
   private val binaryEncoderPool: BinaryEncoderPool,
   private val decoderFactory: DecoderFactory,
) : Serde<T> {

   companion object {
      inline operator fun <reified T : Any> invoke(
         encoderFactory: EncoderFactory,
         decoderFactory: DecoderFactory,
      ): BinarySerde<T> {
         return invoke(T::class, encoderFactory, decoderFactory)
      }

      operator fun <T : Any> invoke(
         kclass: KClass<T>,
         encoderFactory: EncoderFactory,
         decoderFactory: DecoderFactory,
      ): BinarySerde<T> {
         val schema = ReflectionSchemaBuilder(true).schema(kclass)
         val encoder = ReflectionRecordEncoder(schema, kclass)
         val decoder = ReflectionRecordDecoder(schema, kclass)
         val pool = BinaryEncoderPool(Int.MAX_VALUE, encoderFactory)
         return BinarySerde(schema, encoder, decoder, pool, decoderFactory)
      }
   }

   override fun serialize(obj: T): ByteArray {
      val baos = ByteArrayOutputStream()
      binaryEncoderPool.use(baos) {
         val writer = BinaryWriter(schema, baos, it, encoder)
         writer.use {
            writer.write(obj)
            writer.close()
         }
      }
      return baos.toByteArray()
   }

   override fun deserialize(bytes: ByteArray): T {
      return BinaryReader(schema, ByteArrayInputStream(bytes), decoderFactory, decoder, null).use { it.read() }
   }
}
