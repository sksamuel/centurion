package com.sksamuel.centurion.avro.io.serde

import com.sksamuel.centurion.avro.decoders.Decoder
import com.sksamuel.centurion.avro.encoders.Encoder
import com.sksamuel.centurion.avro.io.BinaryReaderFactory
import com.sksamuel.centurion.avro.io.BinaryWriterFactory
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory

/**
 * A [DataSerde] reads and writes in the avro "data" format which includes the schema in the written bytes.
 */
class DataSerde<T : Any>(
  private val schema: Schema,
  encoder: Encoder<T>,
  decoder: Decoder<T>,
  options: SerdeOptions
) : Serde<T> {

   private val encoderFactory = EncoderFactory()
      .configureBufferSize(options.encoderBufferSize)
      .configureBlockSize(options.blockBufferSize)

   private val decoderFactory = DecoderFactory()
      .configureDecoderBufferSize(options.decoderBufferSize)

   private val writerFactory = BinaryWriterFactory(encoderFactory)
   private val readerFactory = BinaryReaderFactory(decoderFactory)

   private val encoderFn = encoder.encode(schema)
   private val decodeFn = decoder.decode(schema)

   override fun serialize(obj: T): ByteArray {
      return writerFactory.writer(schema).use { it.write(encoderFn(obj) as GenericRecord).bytes() }
   }

   override fun deserialize(bytes: ByteArray): T {
      return decodeFn(readerFactory.reader(schema, bytes).use(decodeFn))
   }
}
