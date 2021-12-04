package com.sksamuel.centurion.parquet.converters

import com.sksamuel.centurion.Schema
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter

class MapConverter(
  private val schema: Schema.Map,
  private val index: Int,
  private val collector: ValuesCollector,
) : GroupConverter() {

  private val keys = RepeatedValuesCollector()
  private val values = RepeatedValuesCollector()

  override fun getConverter(fieldIndex: Int): Converter {
    require(fieldIndex == 0) // maps only ever have a single key_value field inside the array

    return object : GroupConverter() {
      override fun getConverter(fieldIndex: Int): Converter {
        return when (fieldIndex) {
          0 -> Converters.converterFor(Schema.Strings, -1, keys)
          1 -> Converters.converterFor(schema.values, -1, values)
          else -> error("Out of bounds map converter index $fieldIndex")
        }
      }

      override fun end() {}
      override fun start() {}
    }
  }

  override fun start() {
    keys.reset()
    values.reset()
  }

  override fun end() {
    val map = keys.values().zip(values.values()).toMap()
    collector[index] = map
  }
}
