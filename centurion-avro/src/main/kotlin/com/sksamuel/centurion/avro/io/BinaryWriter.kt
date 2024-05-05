package com.sksamuel.centurion.avro.io

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import java.io.OutputStream

/**
 * Creates a [BinaryWriterFactory] for a given schema which can then be used to create [BinaryWriter]s.
 *
 * All writers created from this factory share a thread safe [DatumWriter].
 *
 * Pass in a pre-created [EncoderFactory] if you wish to configure buffer size.
 */
class BinaryWriterFactory(
   private val factory: EncoderFactory,
) {

   /**
    * Creates a [BinaryWriterFactory] with the default [EncoderFactory].
    */
   constructor() : this(EncoderFactory.get())

   /**
    * Creates a [BinaryWriter] that writes to the given [OutputStream].
    */
   fun writer(schema: Schema, output: OutputStream): BinaryWriter {
      return BinaryWriter(schema, output, factory)
   }

   /**
    * Creates a [BinaryWriter] that uses a [ByteArrayOutputStream].
    * Once records have been written, users can call bytes() to retrieve the [ByteArray].
    */
   fun writer(schema: Schema): BinaryWriter {
      return BinaryWriter(schema, ByteArrayOutputStream(), factory)
   }

   /**
    * Creates an avro encoded byte array from the given [record].
    *
    * This method is a convenience function that is useful when you want to write a single record
    * in a single method call.
    */
   fun toBytes(record: GenericRecord): ByteArray {
      return BinaryWriter(record.schema, ByteArrayOutputStream(), EncoderFactory.get())
         .use { it.write(record) }
         .bytes()
   }
}

/**
 * A [BinaryWriter] is a non-thread safe, one time use, writer to a given stream that encodes
 * in the avro "binary" format, which does not include the schema in the final bytes.
 *
 * Call [close] when all records have been written to ensure data is flushed to the underlying stream.
 */
class BinaryWriter(
   schema: Schema,
   private val output: OutputStream,
   factory: EncoderFactory,
) : AutoCloseable {

   private val datum = GenericDatumWriter<GenericRecord>(schema)
   private val encoder = factory.binaryEncoder(output, null)

   fun write(record: GenericRecord): BinaryWriter {
      datum.write(record, encoder)
      return this
   }

   private fun flush() {
      encoder.flush()
   }

   override fun close() {
      flush()
      output.close()
   }

   /**
    * Returns the bytes written as a [ByteArray].
    *
    * Throws an exception if this [BinaryWriter] was *not* instantiated with a [ByteArrayOutputStream].
    */
   fun bytes(): ByteArray {
      flush()
      return (output as ByteArrayOutputStream).toByteArray()
   }
}
