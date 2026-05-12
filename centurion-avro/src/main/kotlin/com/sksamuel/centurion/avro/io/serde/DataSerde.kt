package com.sksamuel.centurion.avro.io.serde

import com.sksamuel.centurion.avro.decoders.Decoder
import com.sksamuel.centurion.avro.decoders.ReflectionRecordDecoder
import com.sksamuel.centurion.avro.encoders.Encoder
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.schemas.ReflectionSchemaBuilder
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
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
 *
 * Instances are safe to share across threads. The per-thread [ByteArrayOutputStream] is held in a
 * [ThreadLocal] and reset between calls; the schema-bound [GenericDatumWriter] / [GenericDatumReader]
 * are shared. The Avro [DataFileWriter] / [DataFileReader] still bind to a specific stream, so they
 * are created per call.
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

   private val datumWriter = GenericDatumWriter<GenericRecord>(schema)
   private val datumReader = GenericDatumReader<GenericRecord>(schema, schema)

   private val baosLocal = ThreadLocal.withInitial { ByteArrayOutputStream(256) }

   override fun serialize(obj: T): ByteArray {
      val baos = baosLocal.get()
      baos.reset()
      val record = encoder.encode(schema, obj) as GenericRecord
      val writer = DataFileWriter(datumWriter)
      if (codecFactory != null) writer.setCodec(codecFactory)
      writer.create(schema, baos).use { it.append(record) }
      return baos.toByteArray()
   }

   override fun deserialize(bytes: ByteArray): T {
      val input = SeekableByteArrayInput(bytes)
      return DataFileReader.openReader(input, datumReader).use { reader ->
         decoder.decode(schema, reader.next())
      }
   }
}
