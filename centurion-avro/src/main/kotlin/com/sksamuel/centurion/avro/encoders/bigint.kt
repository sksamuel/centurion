package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import java.math.BigInteger
import java.nio.ByteBuffer

/**
 * An [Encoder] for [BigInteger] that encodes as Avro BYTES using
 * [BigInteger.toByteArray]'s two's-complement big-endian representation.
 * The encoded form round-trips losslessly with [BigIntegerBytesDecoder].
 */
object BigIntegerBytesEncoder : Encoder<BigInteger> {
   override fun encode(schema: Schema, value: BigInteger): Any? {
      require(schema.type == Schema.Type.BYTES) {
         "BigIntegerBytesEncoder requires a BYTES schema, was ${schema.type}"
      }
      return ByteBuffer.wrap(value.toByteArray())
   }
}

/**
 * An [Encoder] for [BigInteger] that encodes as a decimal Avro [Schema.Type.STRING].
 */
object BigIntegerStringEncoder : Encoder<BigInteger> {
   override fun encode(schema: Schema, value: BigInteger): Any? {
      require(schema.type == Schema.Type.STRING) {
         "BigIntegerStringEncoder requires a STRING schema, was ${schema.type}"
      }
      return StringEncoder.encode(schema, value.toString())
   }
}
