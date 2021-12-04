package com.sksamuel.centurion.parquet.converters

import com.sksamuel.centurion.StructBuilder
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter

/**
 * Similar to Parquet's SimplePrimitiveConverter, this is a [PrimitiveConverter]
 * that sets values on a [StructBuilder] for a particular field.
 */
class StructBuilderPrimitiveConverter(
  private val index: Int,
  private val collector: ValuesCollector,
) : PrimitiveConverter() {

  override fun addBinary(x: Binary) {
    collector[index] = x
  }

  override fun addBoolean(x: Boolean) {
     collector[index] = x
  }

  override fun addDouble(x: Double) {
     collector[index] = x
  }

  override fun addFloat(x: Float) {
     collector[index] = x
  }

  override fun addInt(x: Int) {
     collector[index] = x
  }

  override fun addLong(x: Long) {
     collector[index] = x
  }
}
