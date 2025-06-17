package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

class GenericRecordEncoder<T>(val encoder: Encoder<T>) : Encoder<T> {
   override fun encode(schema: Schema, value: T): GenericRecord = encoder.encode(schema, value) as GenericRecord
}
