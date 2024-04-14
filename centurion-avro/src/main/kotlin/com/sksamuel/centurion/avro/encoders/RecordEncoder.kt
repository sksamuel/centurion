package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.decoders.Decoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

class RecordEncoder<T>(val schema: Schema, private val encoder: Encoder<T>) {
   fun encode(value: T): GenericRecord = encoder.encode(schema, value) as GenericRecord
}

class RecordDecoder<T>(val schema: Schema, private val decoder: Decoder<T>) {
   fun decode(record: GenericRecord): T = decoder.decode(schema, record)
}
