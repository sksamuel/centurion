package com.sksamuel.centurion.avro.io

/**
 * A [Serde] provides an easy way to convert between a data class [T] and avro encoded bytes.
 */
interface Serde<T : Any> {
   fun serialize(obj: T): ByteArray
   fun deserialize(bytes: ByteArray): T
}
