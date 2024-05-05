package com.sksamuel.centurion.avro.io

import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumWriter
import java.io.ByteArrayOutputStream
import java.io.OutputStream

/**
 * Creates a [DataWriterFactory] for a given schema which can then be used to create [DataWriter]s.
 *
 * All writers created from this factory share a thread safe [DatumWriter].
 */
class DataWriterFactory(
   private val schema: Schema,
   private val codec: CodecFactory,
) {

   companion object {

      /**
       * Creates an avro encoded byte array from the given [record].
       *
       * This method is a convenience function that is useful when you want to write a single record
       * in a single method call.
       *
       * For better performance, considering creating a [DataWriterFactory] which will use
       * a shared [GenericDatumWriter].
       */
      fun toBytes(record: GenericRecord, codec: CodecFactory = CodecFactory.nullCodec()): ByteArray {
         val datum = GenericDatumWriter<GenericRecord>(record.schema)
         val writer = DataWriter(datum, record.schema, ByteArrayOutputStream(), codec)
         writer.write(record)
         writer.close()
         return writer.bytes()
      }

      /**
       * Creates an avro encoded byte array from the given [record]s.
       *
       * This method is a convenience function that is useful when you want to write a batch
       * of records in a single method call.
       *
       * For better performance, considering creating a [DataWriterFactory] which will use
       * a shared [GenericDatumWriter].
       */
      fun toBytes(records: List<GenericRecord>, codec: CodecFactory = CodecFactory.nullCodec()): ByteArray {
         require(records.isNotEmpty())
         val datumWriter = GenericDatumWriter<GenericRecord>(records.first().schema)
         val writer = DataWriter(datumWriter, records.first().schema, ByteArrayOutputStream(), codec)
         records.forEach { writer.write(it) }
         writer.close()
         return writer.bytes()
      }
   }

   private val datum = GenericDatumWriter<GenericRecord>(schema)

   /**
    * Creates a [DataWriter] that writes to the given [OutputStream].
    * Calling close on the created writer will close this stream and ensure data is flushed.
    */
   fun writer(output: OutputStream): DataWriter {
      return DataWriter(datum, schema, output, codec)
   }

   /**
    * Creates a [DataWriter] that uses a [ByteArrayOutputStream].
    * Once records have been written, users can call bytes() to retrieve the [ByteArray].
    */
   fun writer(): DataWriter {
      return DataWriter(datum, schema, ByteArrayOutputStream(), codec)
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
   datum: DatumWriter<GenericRecord>,
   schema: Schema,
   private val output: OutputStream,
   codecFactory: CodecFactory,
) : AutoCloseable {

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
