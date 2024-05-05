package com.sksamuel.centurion.avro.io

import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import java.io.ByteArrayOutputStream
import java.io.OutputStream

/**
 * Creates a [DataWriterFactory] which can then be used to create [DataWriter]s.
 */
class DataWriterFactory(
   private val codec: CodecFactory,
) {

   /**
    * Creates a [DataWriter] that writes to the given [OutputStream].
    * Calling close on the created writer will close this stream and ensure data is flushed.
    */
   fun writer(schema: Schema, output: OutputStream): DataWriter {
      return DataWriter( schema, output, codec)
   }

   /**
    * Creates a [DataWriter] that uses a [ByteArrayOutputStream].
    * Once records have been written, users can call bytes() to retrieve the [ByteArray].
    */
   fun writer(schema: Schema): DataWriter {
      return DataWriter(schema, ByteArrayOutputStream(), codec)
   }

   /**
    * Creates an avro encoded byte array from the given [record].
    *
    * This method is a convenience function that is useful when you want to write a single record
    * in a single method call.
    */
   fun toBytes(record: GenericRecord): ByteArray {
      return toBytes(listOf(record))
   }

   /**
    * Creates an avro encoded byte array from the given [record]s.
    *
    * This method is a convenience function that is useful when you want to write a batch
    * of records in a single method call.
    */
   fun toBytes(records: List<GenericRecord>): ByteArray {
      require(records.isNotEmpty())
      val writer = DataWriter(records.first().schema, ByteArrayOutputStream(), codec)
      records.forEach { writer.write(it) }
      writer.close()
      return writer.bytes()
   }
}

/**
 * A [DataWriter] is a non-thread safe, one time use, writer to a given stream, that encodes
 * Avro [GenericRecord]s as data-encoded bytes (that is, includes the schema along with the binary).
 *
 * Call [close] when all records have been written to ensure data is flushed to the underlying stream.
 *
 * If you do not want to include the schema, see [BinaryWriter].
 */
class DataWriter(
   schema: Schema,
   private val output: OutputStream,
   codecFactory: CodecFactory,
) : AutoCloseable {

   private val datum = GenericDatumWriter<GenericRecord>(schema)
   private val writer = DataFileWriter(datum).setCodec(codecFactory).create(schema, output)

   fun write(record: GenericRecord): DataWriter {
      writer.append(record)
      return this
   }

   private fun flush() {
      writer.flush()
   }

   override fun close() {
      writer.close()
      output.close()
   }

   /**
    * Returns the bytes written as a [ByteArray].
    *
    * Throws an exception if this [DataWriter] was *not* instantiated with a [ByteArrayOutputStream].
    */
   fun bytes(): ByteArray {
      close()
      return (output as ByteArrayOutputStream).toByteArray()
   }
}
