package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.encoders.Encoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryEncoder
import java.io.OutputStream

/**
 * A [BinaryWriter] is a non-thread safe, writer to a given stream, that encodes objects of type T
 * in the Avro 'binary' format (that is, the schema is not included in the output).
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
class BinaryWriter<T>(
   private val schema: Schema,
   private val out: OutputStream,
   private val binaryEncoder: BinaryEncoder,
   private val encoder: Encoder<T>,
) : AutoCloseable {

   private val datum = GenericDatumWriter<GenericRecord>(schema)

   fun write(obj: T) {
      val record = encoder.encode(schema, obj) as GenericRecord
      datum.write(record, binaryEncoder)
   }

   override fun close() {
      binaryEncoder.flush()
      out.close()
   }
}
