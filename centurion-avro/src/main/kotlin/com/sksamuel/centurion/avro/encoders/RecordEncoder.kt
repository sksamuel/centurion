package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.decoders.Decoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

/**
 * Wrapper around an [Encoder] for data classes, that returns a [GenericRecord] without needing casting.
 */
class RecordEncoder<T>(val schema: Schema, private val encoder: Encoder<T>) {
   fun encode(value: T): GenericRecord = encoder.encode(schema, value) as GenericRecord
}

/**
 * Wrapper around a [Decoder] for data classes, that accepts a [GenericRecord] and extracts the schema.
 */
class RecordDecoder<T>(private val decoder: Decoder<T>) {
   fun decode(record: GenericRecord): T = decoder.decode(record.schema, record)
}
