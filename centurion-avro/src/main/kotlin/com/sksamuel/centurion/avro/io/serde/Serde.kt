package com.sksamuel.centurion.avro.io.serde

/**
 * A [Serde] provides a convenience wrapper to convert between a single data class instance
 * of type [T] and avro encoded byte arrays.
 *
 * It is intended as an easy-to-use alternative to manually managing input/output streams and
 * datum reader/writers and all that jazz when you simply want to read and write a single record.
 */
interface Serde<T : Any> {
   fun serialize(obj: T): ByteArray
   fun deserialize(bytes: ByteArray): T
}

