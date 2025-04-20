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
 * A [BinarySerde] reads and writes in the avro "binary" format which does not include the schema
 * in the written bytes. This results in a smaller payload, similar to protobuf, but requires
 * that the deserializer knows which schema was used when the payload was written. So this serde
 * always requires the schema to be specified.
 */
class BinarySerde<T : Any>(
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

   private val encoderFn = encoder
   private val decodeFn = decoder.decode(schema)

   override fun serialize(obj: T): ByteArray {
      return writerFactory.toBytes(encoderFn.encode(schema, obj) as GenericRecord)
   }

   override fun deserialize(bytes: ByteArray): T {
      return decodeFn(readerFactory.fromBytes(schema, bytes))
   }
}
