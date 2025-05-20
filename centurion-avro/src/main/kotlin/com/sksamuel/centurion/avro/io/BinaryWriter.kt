package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.encoders.Encoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.EncoderFactory
import java.io.OutputStream

/**
 * A [BinaryWriter] is a non-thread safe, writer to a given stream, that encodes Avro [GenericRecord]s
 * as binary-encoded bytes (that is, the schema is not included in the output).
 *
 * If you want to include the schema, see [DataWriter].
 *
 * Call [close] when all records have been written to ensure data is flushed to the underlying stream.
 */
class BinaryWriter<T>(
   private val schema: Schema,
   private val output: OutputStream,
   private val encoder: Encoder<T>,
   factory: EncoderFactory,
   reuse: BinaryEncoder?,
) : AutoCloseable {

   private val datum = GenericDatumWriter<GenericRecord>(schema)
   private val binaryEncoder = factory.binaryEncoder(output, reuse)

   fun write(obj: T) {
      val record = encoder.encode(schema, obj) as GenericRecord
      datum.write(record, binaryEncoder)
   }

   override fun close() {
      binaryEncoder.flush()
      output.close()
   }
}
