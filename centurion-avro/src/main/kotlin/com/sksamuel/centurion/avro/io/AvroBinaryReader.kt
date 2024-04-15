package com.sksamuel.centurion.avro.io

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DecoderFactory
import java.io.InputStream

/**
 * Creates an [AvroBinaryReaderFactory] for a given schema which can then be used
 * to create [AvroBinaryReader]s. All reader created from this factory share a thread safe [DatumReader].
 *
 * Pass in a precreated [DecoderFactory] if you wish to configure buffer size.
 */
class AvroBinaryReaderFactory(schema: Schema, private val factory: DecoderFactory) {
   constructor(schema: Schema) : this(schema, DecoderFactory.get())

   private val datumReader = GenericDatumReader<GenericRecord>(schema)

   /**
    * Creates an [AvroBinaryReader] that reads from the given [InputStream].
    * This variant is much slower than using a byte array. If you already have
    * the bytes available, that should be preferred.
    */
   fun writer(input: InputStream): AvroBinaryReader {
      return AvroBinaryReader(datumReader, input, null, factory)
   }
}

/**
 * An [AvroBinaryReader] is a non-thread safe, one time use, reader from a given stream or byte array.
 */
class AvroBinaryReader(
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

   fun read(): GenericRecord? {
      return datumReader.read(null, decoder)
   }

   override fun close() {
      input?.close()
   }
}
