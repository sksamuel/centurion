package com.sksamuel.centurion.avro.io

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.EncoderFactory
import java.io.OutputStream

/**
 * A [BinaryWriter] is a non-thread safe, writer to a given stream, that encodes Avro [GenericRecord]s
 * in the 'binary' format (that is, the schema is not included in the output).
 *
 * This results in a smaller payload, similar to protobuf, but requires that the schema is provided at
 * deserialization time. This format is especially effective when the consumers and producers both know the
 * schema that was used, for instance, in versioned endpoints, or when the same application is used to read
 * and write the data.
 *
 * If you want to include the schema, see [DataWriter].
 *
 * Call [close] when all records have been written to ensure data is flushed to the underlying stream.
 */
class BinaryWriter(
   private val schema: Schema,
   private val output: OutputStream,
   factory: EncoderFactory,
   reuse: BinaryEncoder?,
) : AutoCloseable {

   private val datum = GenericDatumWriter<GenericRecord>(schema)
   private val binaryEncoder = factory.binaryEncoder(output, reuse)

   fun write(record: GenericRecord) {
      require(record.schema.fullName == schema.fullName) {
         "Record schema ${record.schema.fullName} does not match writer schema ${schema.fullName}"
      }
      datum.write(record, binaryEncoder)
   }

   override fun close() {
      binaryEncoder.flush()
      output.close()
   }
}
