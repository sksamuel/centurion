package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.decoders.Decoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.DecoderFactory
import java.io.InputStream

/**
 * A [BinaryReader]
 */
class BinaryReader<T>(
   writerSchema: Schema,
   readerSchema: Schema,
   private val input: InputStream,
   factory: DecoderFactory,
   val decoder: Decoder<T>,
   reuse: BinaryDecoder?,
) : AutoCloseable {

   constructor(
      schema: Schema,
      input: InputStream,
      factory: DecoderFactory,
      decoder: Decoder<T>,
      reuse: BinaryDecoder?,
   ) : this(schema, schema, input, factory, decoder, reuse)

   private val datum = GenericDatumReader<GenericRecord>(/* writer = */ writerSchema, /* reader = */ readerSchema)
   private val binaryDecoder = factory.binaryDecoder(input, reuse)

   fun read(): T {
      val record = datum.read(null, binaryDecoder)
      return decoder.decode(record.schema, record)
   }

   override fun close() {
      input.close()
   }
}
