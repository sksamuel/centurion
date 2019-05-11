package com.sksamuel.reactivehive.parquet.converters

import com.sksamuel.reactivehive.StructField
import org.apache.parquet.io.api.PrimitiveConverter
import java.time.LocalDate

class DateConverter(private val field: StructField,
                    private val builder: MutableMap<String, Any?>) : PrimitiveConverter() {

  override fun addInt(value: Int) {
    val date = LocalDate.ofEpochDay(value.toLong())
    builder[field.name] = date
  }
}