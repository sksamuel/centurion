package com.sksamuel.centurion.avro.lettuce

import com.sksamuel.centurion.avro.decoders.ReflectionRecordDecoder
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.schemas.ReflectionSchemaBuilder
import io.lettuce.core.codec.RedisCodec
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import kotlin.reflect.KClass

class ReflectionDataClassCodec<T : Any>(
   private val encoderFactory: EncoderFactory,
   private val decoderFactory: DecoderFactory,
   kclass: KClass<T>,
) : RedisCodec<T, T> {

   private val schema = ReflectionSchemaBuilder().schema(kclass)
   private val encoder = ReflectionRecordEncoder(schema, kclass)
   private val decoder = ReflectionRecordDecoder(schema, kclass)

   init {
      GenericData.get().setFastReaderEnabled(true)
   }

   override fun decodeKey(buffer: ByteBuffer): T {
      return decoder.decode(schema, read(buffer))
   }

   override fun decodeValue(buffer: ByteBuffer): T {
      return decoder.decode(schema, read(buffer))
   }

   override fun encodeKey(key: T): ByteBuffer {
      return write(encoder.encode(schema, key) as GenericRecord)
   }

   override fun encodeValue(value: T): ByteBuffer {
      return write(encoder.encode(schema, value) as GenericRecord)
   }

   private fun read(buffer: ByteBuffer): GenericRecord {
      val datum = GenericDatumReader<GenericRecord>(/* writer = */ schema, /* reader = */ schema)
      val bytes: ByteArray = when (buffer.hasArray()) {
         true -> buffer.array()
         else -> {
            val bytesArray = ByteArray(buffer.remaining())
            buffer.get(bytesArray, 0, bytesArray.size)
            bytesArray
         }
      }
      val decoder = decoderFactory.binaryDecoder(ByteArrayInputStream(bytes), null)
      return datum.read(null, decoder)
   }

   private fun write(record: GenericRecord): ByteBuffer {
      val datum = GenericDatumWriter<GenericRecord>(record.schema)
      val baos = ByteArrayOutputStream()
      val encoder = encoderFactory.binaryEncoder(baos, null)
      datum.write(record, encoder)
      encoder.flush()
      return ByteBuffer.wrap(baos.toByteArray())
   }
}
