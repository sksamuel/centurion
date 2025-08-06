package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Conversions
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.math.BigDecimal

/**
 * An [[Encoder]] for [[BigDecimal]] that encodes as byte arrays.
 */
object BigDecimalBytesEncoder : Encoder<BigDecimal> {

   private val converter = Conversions.DecimalConversion()

   override fun encode(schema: Schema, value: BigDecimal): Any? {
      require(schema.type == Schema.Type.BYTES)

      val logical = schema.logicalType as LogicalTypes.Decimal
      val rm = java.math.RoundingMode.HALF_UP

      return converter.toBytes(value.setScale(logical.scale, rm), schema, logical)
   }
}

/**
 * An [Encoder] for [BigDecimal] that encodes as Strings.
 */
object BigDecimalStringEncoder : Encoder<BigDecimal> {

   override fun encode(schema: Schema, value: BigDecimal): Any? {
      require(schema.type == Schema.Type.STRING)
      return StringEncoder.contraMap<BigDecimal> { it.toString() }
   }
}

/**
 * An [Encoder] for [BigDecimal] that encodes as fixed size byte arrays.
 */
object BigDecimalFixedEncoder : Encoder<BigDecimal> {

   private val converter = Conversions.DecimalConversion()

   override fun encode(schema: Schema, value: BigDecimal): Any? {
      require(schema.type == Schema.Type.FIXED)

      val logical = schema.logicalType as LogicalTypes.Decimal
      val rm = java.math.RoundingMode.HALF_UP

      return converter.toFixed(value.setScale(logical.scale, rm), schema, logical)
   }
}
