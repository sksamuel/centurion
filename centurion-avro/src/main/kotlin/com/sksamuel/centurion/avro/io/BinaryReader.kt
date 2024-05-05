package com.sksamuel.centurion.avro.io

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import java.io.InputStream

/**
 * Creates an [BinaryReaderFactory] for a given schema which can then be used to create [BinaryReader]s.
 * Pass in a pre-created [DecoderFactory] if you wish to configure buffer size.
 */
class BinaryReaderFactory(
   private val factory: DecoderFactory,
) {

   constructor() : this(DecoderFactory.get())

   /**
    * Creates an [BinaryReader] that reads from the given [InputStream].
    *
    * This variant is slower than using a byte array. If you already have
    * the bytes available, that should be preferred.
    */
   fun reader(schema: Schema, input: InputStream): BinaryReader {
      return BinaryReader(schema, schema, input, null, factory)
   }

   /**
    * Creates an [BinaryReader] that reads from the given [InputStream].
    *
    * This variant is slower than using a byte array. If you already have
    * the bytes available, that should be preferred.
    */
   fun reader(reader: Schema, writer: Schema, input: InputStream): BinaryReader {
      return BinaryReader(reader, writer, input, null, factory)
   }

   /**
    * Creates an [BinaryReader] that reads from the given [ByteArray].
    */
   fun reader(schema: Schema, bytes: ByteArray): BinaryReader {
      return BinaryReader(schema, schema, input = null, bytes = bytes, factory = factory)
   }

   /**
    * Creates an [BinaryReader] that reads from the given [ByteArray].
    */
   fun reader(reader: Schema, writer: Schema, bytes: ByteArray): BinaryReader {
      return BinaryReader(readerSchema = reader, writerSchema = writer, input = null, bytes = bytes, factory = factory)
   }

   /**
    * Reads a [GenericRecord] from the given [ByteArray].
    *
    * This method is a convenience function that is useful when you want to read a single record
    * in a single method call.
    *
    * For better performance and customization, create a [BinaryReader] using a [BinaryReaderFactory].
    * This will also allow customization of the [DecoderFactory], schema evolution.
    */
   fun fromBytes(schema: Schema, bytes: ByteArray): GenericRecord {
      return BinaryReader(schema, schema, null, bytes, factory).use { it.read() }
   }
}

/**
 * An [BinaryReader] is a non-thread safe, one time use, reader from a given stream or byte array.
 */
class BinaryReader(
   readerSchema: Schema,
   writerSchema: Schema,
   private val input: InputStream?,
   bytes: ByteArray?,
   factory: DecoderFactory,
) : AutoCloseable {

   init {
      require(input == null || bytes == null) { "Do not specify both ByteArray and InputStream" }
   }

   private val datum = GenericDatumReader<GenericRecord>(/* writer = */ writerSchema, /* reader = */ readerSchema)

   private val decoder = when {
      input != null -> factory.binaryDecoder(input, null)
      bytes != null -> factory.binaryDecoder(bytes, null)
      else -> error("Specify either ByteArray or InputStream")
   }

   fun read(): GenericRecord {
      return datum.read(null, decoder)
   }

   fun asSequence(): Sequence<GenericRecord> {
      return generateSequence { read() }
   }

   override fun close() {
      input?.close()
   }
}
