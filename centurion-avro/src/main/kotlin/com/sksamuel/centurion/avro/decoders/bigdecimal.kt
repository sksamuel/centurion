package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Conversions
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import org.apache.avro.util.Utf8
import java.math.BigDecimal
import java.nio.ByteBuffer

/**
 * A [Decoder] for [BigDecimal] that decodes from byte arrays.
 */
object BigDecimalBytesDecoder : Decoder<BigDecimal> {

   private val converter = Conversions.DecimalConversion()

   override fun decode(schema: Schema, value: Any?): BigDecimal {
      require(schema.type == Schema.Type.BYTES)
      val logical = schema.logicalType as LogicalTypes.Decimal
      return when (value) {
         is ByteBuffer -> converter.fromBytes(value, schema, logical)
         is ByteArray -> converter.fromBytes(ByteBuffer.wrap(value), schema, logical)
         else -> error("Unsupported value for BigDecimal bytes decoder: $value")
      }
   }
}

/**
 * A [Decoder] for [BigDecimal] that decodes from Strings.
 */
object BigDecimalStringDecoder : Decoder<BigDecimal> {
   override fun decode(schema: Schema, value: Any?): BigDecimal {
      require(schema.type == Schema.Type.STRING)
      return when (value) {
         is CharSequence -> BigDecimal(value.toString())
         is Utf8 -> BigDecimal(value.toString())
         else -> error("Unsupported value for BigDecimal string decoder: $value")
      }
   }
}

/**
 * A [Decoder] for [BigDecimal] that decodes from fixed size byte arrays.
 */
object BigDecimalFixedDecoder : Decoder<BigDecimal> {

   private val converter = Conversions.DecimalConversion()

   override fun decode(schema: Schema, value: Any?): BigDecimal {
      require(schema.type == Schema.Type.FIXED)
      val logical = schema.logicalType as LogicalTypes.Decimal
      return when (value) {
         is GenericFixed -> converter.fromFixed(value, schema, logical)
         else -> error("Unsupported value for BigDecimal fixed decoder: $value")
      }
   }
}
