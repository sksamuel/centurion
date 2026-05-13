package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer
import java.util.UUID

/**
 * A [Decoder] for [UUID].
 *
 * Mirrors the encoder side (`Utf8UUIDEncoder` / `JavaStringUUIDEncoder`):
 * the canonical encoded form is a STRING. Accepts any [CharSequence] —
 * which covers both Kotlin [String] and Avro's [Utf8].
 *
 * Also accepts BYTES / FIXED of length 16, treated as a big-endian
 * (RFC 4122 network-byte-order) representation of the UUID, matching
 * the layout used by `UUID.nameUUIDFromBytes(...)` round-trips.
 */
object UUIDDecoder : Decoder<UUID> {
   override fun decode(schema: Schema, value: Any?): UUID {
      return when (value) {
         is CharSequence -> UUID.fromString(value.toString())
         is ByteArray -> fromBytes(value)
         is GenericFixed -> fromBytes(value.bytes())
         is ByteBuffer -> {
            val bytes = ByteArray(value.remaining())
            value.duplicate().get(bytes)
            fromBytes(bytes)
         }
         else -> error("Cannot decode ${value?.javaClass?.name} as UUID: $value")
      }
   }

   private fun fromBytes(bytes: ByteArray): UUID {
      require(bytes.size == 16) { "UUID byte representation must be exactly 16 bytes, was ${bytes.size}" }
      val buffer = ByteBuffer.wrap(bytes)
      val msb = buffer.long
      val lsb = buffer.long
      return UUID(msb, lsb)
   }
}
