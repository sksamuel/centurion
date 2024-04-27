package com.sksamuel.centurion.avro.io

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import java.io.OutputStream

///**
// * An [AvroWriter] will write [GenericRecord]s to an output stream.
// *
// * There are three implementations of this stream
// *  - a Data stream,
// *  - a Binary stream
// *  - a Json stream
// *
// * See the methods on the companion object to create instances of each
// * of these types of stream.
// */

/**
 * Creates an [BinaryWriterFactory] for a given schema which can then be used
 * to create [BinaryWriter]s. All writers created from this factory share a thread safe [DatumWriter].
 *
 * Pass in a pre-created [EncoderFactory] if you wish to configure buffer size.
 */
class BinaryWriterFactory(schema: Schema, private val factory: EncoderFactory) {
   constructor(schema: Schema) : this(schema, EncoderFactory.get())

   private val datumWriter = GenericDatumWriter<GenericRecord>(schema)

   /**
    * Creates an [BinaryWriter] that writes to the given [OutputStream].
    * Calling close on the created writer will close this stream and ensure data is flushed.
    */
   fun writer(output: OutputStream): BinaryWriter {
      return BinaryWriter(datumWriter, output, factory)
   }

   /**
    * Creates an [BinaryWriter] that uses a [ByteArrayOutputStream].
    * Once records have been written, users can call bytes() to retrieve the [ByteArray].
    */
   fun writer(): BinaryWriter {
      return BinaryWriter(datumWriter, ByteArrayOutputStream(), factory)
   }

   /**
    * Creates an avro encoded byte array from the given [record].
    * This method is a convenience function that is useful when you want to write a single record.
    * If you wish to write multiple records, create a [BinaryWriter] using [writer].
    */
   fun write(record: GenericRecord): ByteArray {
      return BinaryWriter(datumWriter, ByteArrayOutputStream(), factory).use { it.write(record) }.bytes()
   }

   /**
    * Writes avro encoded bytes to the given [output] stream from the given [record].
    * This method is a convenience function that is useful when you want to write a single record.
    * If you wish to write multiple records, create a [BinaryWriter] using [writer].
    *
    * The given [output] stream will be closed after this function returns.
    */
   fun write(record: GenericRecord, output: OutputStream) {
      BinaryWriter(datumWriter, output, factory).use { it.write(record) }
   }
}

/**
 * An [BinaryWriter] is a non-thread safe, one time use, writer to a given stream.
 * Call [close] when all records have been written to ensure data is flushed to the underlying stream.
 */
class BinaryWriter(
   private val datumWriter: DatumWriter<GenericRecord>,
   private val output: OutputStream,
   factory: EncoderFactory,
) : AutoCloseable {

   private val encoder = factory.binaryEncoder(output, null)

   fun write(record: GenericRecord): BinaryWriter {
      datumWriter.write(record, encoder)
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
    * Returns the bytes written. Throws an exception if this [BinaryWriter] was *not*
    * instantiated with a [ByteArrayOutputStream].
    */
   fun bytes(): ByteArray {
      flush()
      return (output as ByteArrayOutputStream).toByteArray()
   }
}