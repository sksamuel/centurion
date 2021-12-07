package com.sksamuel.centurion.parquet.converters

import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter

class EnumConverter(
  private val index: Int,
  private val collector: ValuesCollector
) : PrimitiveConverter() {

  override fun addBinary(binary: Binary) {
    val value = binary.toStringUsingUTF8()
    collector[index] = value
  }

}
