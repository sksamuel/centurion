package com.sksamuel.centurion.avro.generation

import org.apache.avro.generic.GenericRecord

/**
 * A [GenericRecordSerializer] serializes a data class of type [T] into a [GenericRecord].
 */
fun interface GenericRecordSerializer<T> {
   fun encode(value: T): GenericRecord
}
