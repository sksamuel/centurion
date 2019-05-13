package com.sksamuel.rxhive.parquet.converters

import com.sksamuel.rxhive.Precision
import com.sksamuel.rxhive.Scale
import com.sksamuel.rxhive.StructField
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