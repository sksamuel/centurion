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
 * Creates an [AvroBinaryWriterFactory] for a given schema which can then be used
 * to create [AvroBinaryWriter]s. These writers share a thread safe [DatumWriter].
 */
class AvroBinaryWriterFactory(schema: Schema, private val factory: EncoderFactory) {
   constructor(schema: Schema) : this(schema, EncoderFactory.get())

   private val datumWriter = GenericDatumWriter<GenericRecord>(schema)

   /**
    * Creates an [AvroBinaryWriter] that writes to the given [OutputStream].
    * Calling close on the created writer will close this stream and ensure data is flushed.
    */
   fun writer(output: OutputStream): AvroBinaryWriter {
      return AvroBinaryWriter(datumWriter, output, factory)
   }

   /**
    * Creates an [AvroBinaryWriter] that uses a [ByteArrayOutputStream].
    * Once records have been written, users can call bytes() to retrieve the [ByteArray].
    */
   fun writer(): AvroBinaryWriter {
      return AvroBinaryWriter(datumWriter, ByteArrayOutputStream(), factory)
   }
}

class AvroBinaryWriter(
   private val datumWriter: DatumWriter<GenericRecord>,
   private val output: OutputStream,
   factory: EncoderFactory,
) : AutoCloseable {

   private val encoder = factory.binaryEncoder(output, null)

   fun write(record: GenericRecord) {
      datumWriter.write(record, encoder)
   }

   private fun flush() {
      encoder.flush()
   }

   override fun close() {
      flush()
      output.close()
   }

   fun bytes(): ByteArray {
      flush()
      return (output as ByteArrayOutputStream).toByteArray()
   }
}
