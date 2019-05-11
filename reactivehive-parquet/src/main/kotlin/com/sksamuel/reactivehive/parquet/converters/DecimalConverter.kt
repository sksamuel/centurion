package com.sksamuel.reactivehive.parquet.converters

import com.sksamuel.reactivehive.Precision
import com.sksamuel.reactivehive.Scale
import com.sksamuel.reactivehive.StructField
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter
import java.math.BigDecimal
import java.math.BigInteger
import java.math.MathContext

class DecimalConverter(private val field: StructField,
                       private val precision: Precision,
                       private val scale: Scale,
                       private val builder: MutableMap<String, Any?>) : PrimitiveConverter() {
  override fun addBinary(value: Binary) {
    val bigint = BigInteger(value.bytes)
    val decimal = BigDecimal(bigint, scale.value, MathContext(precision.value))
    builder[field.name] = decimal
  }
}