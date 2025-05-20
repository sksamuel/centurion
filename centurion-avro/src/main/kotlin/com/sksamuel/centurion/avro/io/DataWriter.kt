package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.encoders.Encoder
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import java.io.OutputStream

/**
 * A [DataWriter] is a non-thread safe, writer to a given stream, that encodes Avro [GenericRecord]s
 * as data-encoded bytes (that is, the schema is included in the output along with the data).
 *
 * If you do not want to include the schema, see [BinaryWriter].
 *
 * Call [close] when all records have been written to ensure data is flushed to the underlying stream.
 */
class DataWriter<T>(
  private val schema: Schema,
  private val output: OutputStream,
  private val encoder: Encoder<T>,
  codecFactory: CodecFactory?,
) : AutoCloseable {

  private val datum = GenericDatumWriter<GenericRecord>(schema)
  private val writer = DataFileWriter(datum).setCodec(codecFactory).create(schema, output)

  fun write(obj: T) {
    val record = encoder.encode(schema, obj) as GenericRecord
    writer.append(record)
  }

  private fun flush() {
    writer.flush()
  }

  override fun close() {
    writer.close()
    output.close()
  }
}
