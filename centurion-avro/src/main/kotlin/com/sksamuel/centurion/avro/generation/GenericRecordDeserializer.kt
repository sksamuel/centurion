package com.sksamuel.centurion.avro.generation

import org.apache.avro.generic.GenericRecord

/**
 * A [GenericRecordDeserializer] deserializes a [GenericRecord] into a data class of type [T].
 */
fun interface GenericRecordDeserializer<T> {
   fun decode(record: GenericRecord): T
}
