package com.sksamuel.centurion.avro.io.serde

import com.sksamuel.centurion.avro.decoders.Decoder
import com.sksamuel.centurion.avro.decoders.ReflectionRecordDecoder
import com.sksamuel.centurion.avro.encoders.Encoder
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.schemas.ReflectionSchemaBuilder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
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
 *
 * Instances are safe to share across threads. Per-thread mutable state
 * ([ByteArrayOutputStream], [BinaryEncoder], [BinaryDecoder]) is held in [ThreadLocal]s and reused
 * across calls; the schema-bound [GenericDatumWriter] / [GenericDatumReader] are shared.
 */
class BinarySerde<T : Any>(
   private val schema: Schema,
   private val encoder: Encoder<T>,
   private val decoder: Decoder<T>,
   private val encoderFactory: EncoderFactory,
   private val decoderFactory: DecoderFactory,
) : Serde<T> {

   companion object {

      /**
       * Creates a [BinarySerde] for the given type [T] using reflection to generate the schema,
       * encoder and decoder.
       */
      inline operator fun <reified T : Any> invoke(
         encoderFactory: EncoderFactory,
         decoderFactory: DecoderFactory,
      ): BinarySerde<T> {
         return invoke(T::class, encoderFactory, decoderFactory)
      }

      /**
       * Creates a [BinarySerde] for the given kclass using reflection to generate the schema,
       * encoder and decoder.
       */
      operator fun <T : Any> invoke(
         kclass: KClass<T>,
         encoderFactory: EncoderFactory,
         decoderFactory: DecoderFactory,
      ): BinarySerde<T> {
         val schema = ReflectionSchemaBuilder(true).schema(kclass)
         val encoder = ReflectionRecordEncoder(schema, kclass)
         val decoder = ReflectionRecordDecoder(schema, kclass)
         return BinarySerde(schema, encoder, decoder, encoderFactory, decoderFactory)
      }
   }

   private val datumWriter = GenericDatumWriter<GenericRecord>(schema)
   private val datumReader = GenericDatumReader<GenericRecord>(schema, schema)

   private val baosLocal = ThreadLocal.withInitial { ByteArrayOutputStream(256) }
   private val binaryEncoderLocal = ThreadLocal<BinaryEncoder?>()
   private val binaryDecoderLocal = ThreadLocal<BinaryDecoder?>()

   override fun serialize(obj: T): ByteArray {
      val baos = baosLocal.get()
      baos.reset()
      val binEncoder = encoderFactory.binaryEncoder(baos, binaryEncoderLocal.get())
      binaryEncoderLocal.set(binEncoder)
      val record = encoder.encode(schema, obj) as GenericRecord
      datumWriter.write(record, binEncoder)
      binEncoder.flush()
      return baos.toByteArray()
   }

   override fun deserialize(bytes: ByteArray): T {
      val binDecoder = decoderFactory.binaryDecoder(bytes, 0, bytes.size, binaryDecoderLocal.get())
      binaryDecoderLocal.set(binDecoder)
      val record = datumReader.read(null, binDecoder)
      return decoder.decode(schema, record)
   }
}
