package com.sksamuel.centurion.avro.lettuce

import io.lettuce.core.codec.RedisCodec
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

/**
 * A [RedisCodec] for encoding and decoding [GenericRecord]s using Avro.
 */
class GenericRecordCodec(
   private val schema: Schema,
   private val encoderFactory: EncoderFactory,
   private val decoderFactory: DecoderFactory,
) : RedisCodec<GenericRecord, GenericRecord> {

   private val datumReader = GenericDatumReader<GenericRecord>(/* writer = */ schema, /* reader = */ schema)
   private val datumWriter = GenericDatumWriter<GenericRecord>(schema)

   override fun decodeKey(buffer: ByteBuffer): GenericRecord {
      return decode(buffer)
   }

   override fun decodeValue(buffer: ByteBuffer): GenericRecord {
      return decode(buffer)
   }

   override fun encodeKey(key: GenericRecord): ByteBuffer {
      return encode(key)
   }

   override fun encodeValue(value: GenericRecord): ByteBuffer {
      return encode(value)
   }

   private fun decode(buffer: ByteBuffer): GenericRecord {
      val bytes = ByteArray(buffer.remaining())
      buffer.get(bytes)
      val decoder = decoderFactory.binaryDecoder(bytes, 0, bytes.size, null)
      return datumReader.read(null, decoder)
   }

   private fun encode(record: GenericRecord): ByteBuffer {
      val baos = ByteArrayOutputStream()
      val encoder = encoderFactory.binaryEncoder(baos, null)
      datumWriter.write(record, encoder)
      encoder.flush()
      return ByteBuffer.wrap(baos.toByteArray())
   }
}
