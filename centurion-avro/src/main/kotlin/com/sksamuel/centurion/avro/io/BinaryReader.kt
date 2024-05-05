package com.sksamuel.centurion.avro.io

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DecoderFactory
import java.io.InputStream

/**
 * Creates an [BinaryReaderFactory] for a given schema which can then be used
 * to create [BinaryReader]s.
 *
 * All [BinaryReader]s created from this factory share a thread safe [DatumReader] for efficiency.
 *
 * Pass in a pre-created [DecoderFactory] if you wish to configure buffer size.
 */
class BinaryReaderFactory(
   reader: Schema,
   writer: Schema,
   private val factory: DecoderFactory,
) {

   constructor(schema: Schema) : this(schema, schema, DecoderFactory.get())
   constructor(reader: Schema, writer: Schema) : this(reader, writer, DecoderFactory.get())
   constructor(schema: Schema, factory: DecoderFactory) : this(schema, schema, factory)

   private val datum = GenericDatumReader<GenericRecord>(/* writer = */ writer, /* reader = */ reader)

   companion object {

      /**
       * Reads a [GenericRecord] from the given [bytes].
       *
       * This method is a convenience function that is useful when you want to read a single record.
       * If you wish to read multiple records, create a [BinaryReader] using a [BinaryReaderFactory].
       * This will also allow customization of the [DecoderFactory] and schema evolution.
       */
      fun fromBytes(schema: Schema, bytes: ByteArray): GenericRecord {
         val datumReader = GenericDatumReader<GenericRecord>(/* writer = */ schema, /* reader = */ schema)
         return BinaryReader(
            datumReader = datumReader,
            input = null,
            bytes = bytes,
            factory = DecoderFactory.get(),
         ).read()
      }

      /**
       * Reads a [GenericRecord] from the given [bytes].
       *
       * This method is a convenience function that is useful when you want to read a single record.
       * If you wish to read multiple records, create a [BinaryReader] using a [BinaryReaderFactory].
       * This will also allow customization of the [DecoderFactory] and schema evolution.
       *
       * The given [input] stream will be closed after this function returns.
       *
       * This variant is slower than using a byte array. If you already have
       * the bytes available, that should be preferred.
       */
      fun fromBytes(schema: Schema, input: InputStream): GenericRecord {
         val datumReader = GenericDatumReader<GenericRecord>(/* writer = */ schema, /* reader = */ schema)
         return BinaryReader(datumReader, input, null, DecoderFactory.get()).use { it.read() }
      }
   }

   /**
    * Creates an [BinaryReader] that reads from the given [InputStream].
    *
    * This variant is slower than using a byte array. If you already have
    * the bytes available, that should be preferred.
    */
   fun reader(input: InputStream): BinaryReader {
      return BinaryReader(datum, input, null, factory)
   }

   /**
    * Creates an [BinaryReader] that reads from the given [ByteArray].
    */
   fun reader(bytes: ByteArray): BinaryReader {
      return BinaryReader(
         datumReader = datum,
         input = null,
         bytes = bytes,
         factory = factory
      )
   }
}

/**
 * An [BinaryReader] is a non-thread safe, one time use, reader from a given stream or byte array.
 */
class BinaryReader(
   private val datumReader: DatumReader<GenericRecord>,
   private val input: InputStream?,
   bytes: ByteArray?,
   factory: DecoderFactory,
) : AutoCloseable {

   init {
      require(input == null || bytes == null) { "Do not specify both ByteArray and InputStream" }
   }

   private val decoder = when {
      input != null -> factory.binaryDecoder(input, null)
      bytes != null -> factory.binaryDecoder(bytes, null)
      else -> error("Specify either ByteArray or InputStream")
   }

   fun read(): GenericRecord {
      return datumReader.read(null, decoder)
   }

   override fun close() {
      input?.close()
   }
}
