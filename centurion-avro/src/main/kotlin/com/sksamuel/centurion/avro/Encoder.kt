package com.sksamuel.centurion.avro

import org.apache.avro.generic.GenericRecord

fun interface Encoder<T> {
   fun encode(value: Any): T
}

fun interface GenericRecordEncoder<T> {
   fun encode(value: T): GenericRecord
}
