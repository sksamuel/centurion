package com.sksamuel.reactivehive.parquet.converters

import com.sksamuel.reactivehive.StructField
import com.sksamuel.reactivehive.TimeMillis
import org.apache.parquet.io.api.PrimitiveConverter

class TimeMillisConverter(private val field: StructField,
                          private val builder: MutableMap<String, Any?>) : PrimitiveConverter() {
  override fun addInt(value: Int) {
    builder[field.name] = TimeMillis(value)
  }
}
