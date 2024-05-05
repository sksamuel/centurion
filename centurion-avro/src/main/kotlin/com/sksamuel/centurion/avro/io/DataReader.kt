package com.sksamuel.centurion.avro.io

import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.file.SeekableInput
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DecoderFactory

/**
 * Creates a [DataReaderFactory] for a given schema which can then be used to create [DataReader]s.
 *
 * All readers created from this factory share a thread safe [DatumReader] for efficiency.
 *
 * Pass in a pre-created [DecoderFactory] if you wish to configure buffer size.
 */
class DataReaderFactory(
   reader: Schema,
   writer: Schema,
   private val factory: DecoderFactory,
   private val codecFactory: CodecFactory,
) {

   private val datum = GenericDatumReader<GenericRecord>(reader, writer)

   /**
    * Creates a [DataReader] that reads from the given [ByteArray].
    */
   fun reader(bytes: ByteArray): DataReader {
      return DataReader(SeekableByteArrayInput(bytes), datum)
   }
}

class DataReader(private val input: SeekableInput, datum: DatumReader<GenericRecord>) : AutoCloseable {

   private val reader = DataFileReader.openReader(input, datum)

   fun hasNext() = reader.hasNext()
   fun read(): GenericRecord = reader.next()
   fun iterator() = reader.iterator()

   override fun close() {
      reader.close()
      input.close()
   }
}
