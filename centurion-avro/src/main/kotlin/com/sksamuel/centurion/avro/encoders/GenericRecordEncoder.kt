package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

abstract class GenericRecordEncoder<T> : Encoder<T> {
   abstract override fun encode(schema: Schema, value: T): GenericRecord
}
