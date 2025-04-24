package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.decoders.Decoder
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.file.SeekableInput
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord

/**
 * Creates a reader for a given schema which can then be used to read data classes of type [T]
 */
class DataReader<T>(
   private val input: SeekableInput,
   private val schema: Schema,
   private val decoder: Decoder<T>,
) : AutoCloseable {

   constructor(
      bytes: ByteArray,
      schema: Schema,
      decoder: Decoder<T>
   ) : this(SeekableByteArrayInput(bytes), schema, decoder)

   private val datum = GenericDatumReader<GenericRecord>(schema)
   private val reader = DataFileReader.openReader(input, datum)

   fun hasNext(): Boolean = reader.hasNext()
   fun next(): T = decoder.decode(schema, reader.next())
   fun sequence(): Sequence<T> = reader.asIterable().asSequence().map { decoder.decode(schema, it) }

   override fun close() {
      reader.close()
      input.close()
   }
}

