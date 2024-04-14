package com.sksamuel.centurion.avro.generation

import org.apache.avro.generic.GenericRecord

fun interface GenericRecordEncoder<T> {
   fun encode(value: T): GenericRecord
}

fun interface GenericRecordDecoder<T> {
   fun decode(record: GenericRecord): T
}
