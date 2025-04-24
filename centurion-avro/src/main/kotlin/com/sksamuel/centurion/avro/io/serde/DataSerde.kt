package com.sksamuel.centurion.avro.io.serde

import com.sksamuel.centurion.avro.decoders.Decoder
import com.sksamuel.centurion.avro.encoders.Encoder
import com.sksamuel.centurion.avro.io.DataReader
import com.sksamuel.centurion.avro.io.DataWriter
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import java.io.ByteArrayOutputStream

/**
 * A [DataSerde] reads and writes in the avro "data" format which includes the schema in the written bytes.
 *
 * This format results in larger sizes than [BinarySerde], as clearly including the schema requires
 * more bytes, but supports schema evolution, as the deserializers can compare the expected schema
 * with the written schema.
 */
class DataSerde<T : Any>(
  private val schema: Schema,
  private val encoder: Encoder<T>,
  private val decoder: Decoder<T>,
  private val codecFactory: CodecFactory?,
) : Serde<T> {

  override fun serialize(obj: T): ByteArray {
    val baos = ByteArrayOutputStream()
    DataWriter(schema, baos, encoder, codecFactory).use { it.write(obj) }
    return baos.toByteArray()
  }

  override fun deserialize(bytes: ByteArray): T {
    return DataReader(bytes, schema, decoder).use { it.next() }
  }
}
