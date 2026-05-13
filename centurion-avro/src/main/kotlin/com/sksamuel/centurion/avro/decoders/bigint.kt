package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import java.math.BigInteger
import java.nio.ByteBuffer

/**
 * A [Decoder] for [BigInteger] that decodes from Avro BYTES.
 *
 * Accepts either a [ByteBuffer] or a [ByteArray]; the input is interpreted as
 * the two's-complement big-endian byte representation of the integer, matching
 * [BigInteger.toByteArray].
 *
 * Does not mutate the source [ByteBuffer]'s position.
 */
object BigIntegerBytesDecoder : Decoder<BigInteger> {
   override fun decode(schema: Schema, value: Any?): BigInteger {
      require(schema.type == Schema.Type.BYTES) {
         "BigIntegerBytesDecoder requires a BYTES schema, was ${schema.type}"
      }
      return when (value) {
         is ByteBuffer -> {
            val bytes = ByteArray(value.remaining())
            value.duplicate().get(bytes)
            BigInteger(bytes)
         }
         is ByteArray -> BigInteger(value)
         is GenericFixed -> BigInteger(value.bytes())
         else -> error("Unsupported value for BigInteger bytes decoder: ${value?.javaClass?.name}")
      }
   }
}

/**
 * A [Decoder] for [BigInteger] that decodes from Avro STRINGs.
 */
object BigIntegerStringDecoder : Decoder<BigInteger> {
   override fun decode(schema: Schema, value: Any?): BigInteger {
      require(schema.type == Schema.Type.STRING) {
         "BigIntegerStringDecoder requires a STRING schema, was ${schema.type}"
      }
      return when (value) {
         is CharSequence -> BigInteger(value.toString())
         else -> error("Unsupported value for BigInteger string decoder: ${value?.javaClass?.name}")
      }
   }
}
