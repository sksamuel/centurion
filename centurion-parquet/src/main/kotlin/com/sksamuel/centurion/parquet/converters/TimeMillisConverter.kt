package com.sksamuel.centurion.parquet.converters

import com.sksamuel.centurion.StructBuilder
import com.sksamuel.centurion.TimeMillis
import org.apache.parquet.io.api.PrimitiveConverter

class TimeMillisConverter(
  private val fieldName: String,
  private val builder: StructBuilder
) : PrimitiveConverter() {

  override fun addInt(value: Int) {
    builder[fieldName] = TimeMillis(value)
  }

}
