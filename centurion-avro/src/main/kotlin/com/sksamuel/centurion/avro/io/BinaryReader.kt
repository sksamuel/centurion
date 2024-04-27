package com.sksamuel.centurion.avro.io

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DecoderFactory
import java.io.InputStream

/**
 * Creates an [BinaryReaderFactory] for a given schema which can then be used
 * to create [BinaryReader]s. All reader created from this factory share a thread safe [DatumReader].
 *
 * Pass in a precreated [DecoderFactory] if you wish to configure buffer size.
 */
class BinaryReaderFactory(schema: Schema, private val factory: DecoderFactory) {
   constructor(schema: Schema) : this(schema, DecoderFactory.get())

   private val datumReader = GenericDatumReader<GenericRecord>(schema)

   /**
    * Creates an [BinaryReader] that reads from the given [InputStream].
    *
    * This variant is slower than using a byte array. If you already have
    * the bytes available, that should be preferred.
    */
   fun reader(input: InputStream): BinaryReader {
      return BinaryReader(datumReader, input, null, factory)
   }

   /**
    * Creates an [BinaryReader] that reads from the given [ByteArray].
    */
   fun reader(bytes: ByteArray): BinaryReader {
      return BinaryReader(datumReader, null, bytes, factory)
   }

   fun read(bytes: ByteArray): GenericRecord {
      return BinaryReader(datumReader, null, bytes, factory).read()
   }

   /**
    * Reads avro encoded bytes from the given [input] stream to a [GenericRecord].
    * This method is a convenience function that is useful when you want to read a single record.
    * If you wish to read multiple records, create a [BinaryWriter] using [reader].
    *
    * The given [input] stream will be closed after this function returns.
    *
    * This variant is slower than using a byte array. If you already have
    *the bytes available, that should be preferred.
    */
   fun read(input: InputStream): GenericRecord {
      return BinaryReader(datumReader, input, null, factory).use { it.read() }
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
