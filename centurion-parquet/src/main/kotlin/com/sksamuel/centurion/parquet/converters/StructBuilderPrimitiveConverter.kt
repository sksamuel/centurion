package com.sksamuel.centurion.parquet.converters

import com.sksamuel.centurion.StructBuilder
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter

/**
 * Similar to Parquet's SimplePrimitiveConverter, this is a [PrimitiveConverter]
 * that sets values on a [StructBuilder] for a particular field.
 */
class StructBuilderPrimitiveConverter(
  private val fieldName: String,
  private val builder: StructBuilder,
) : PrimitiveConverter() {

  override fun addBinary(x: Binary) {
    builder.set(fieldName, x.bytes)
  }

  override fun addBoolean(x: Boolean) {
    builder.set(fieldName, x)
  }

  override fun addDouble(x: Double) {
    builder.set(fieldName, x)
  }

  override fun addFloat(x: Float) {
    builder.set(fieldName, x)
  }

  override fun addInt(x: Int) {
    builder.set(fieldName, x)
  }

  override fun addLong(x: Long) {
    builder.set(fieldName, x)
  }
}
