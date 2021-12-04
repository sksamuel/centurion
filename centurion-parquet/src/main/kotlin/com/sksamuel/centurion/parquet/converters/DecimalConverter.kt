package com.sksamuel.centurion.parquet.converters

import com.sksamuel.centurion.Schema
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter
import java.math.BigDecimal
import java.math.BigInteger
import java.math.MathContext

class DecimalConverter(
  private val field: Schema.Field,
  private val precision: Schema.Precision,
  private val scale: Schema.Scale,
  private val builder: MutableMap<String, Any?>
) : PrimitiveConverter() {
  override fun addBinary(value: Binary) {
    val bigint = BigInteger(value.bytes)
    val decimal = BigDecimal(bigint, scale.value, MathContext(precision.value))
    builder[field.name] = decimal
  }
}
