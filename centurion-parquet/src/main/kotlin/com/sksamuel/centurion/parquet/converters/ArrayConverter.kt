package com.sksamuel.centurion.parquet.converters

import com.sksamuel.centurion.Schema
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter

class ArrayConverter(
  private val schema: Schema.Array,
  private val index: Int,
  private val parent: ValuesCollector
) : GroupConverter() {

  private val collector = RepeatedValuesCollector()

  // this converter is for the group called 'list'
  private val converter = object : GroupConverter() {

    // the converter for the underlying element type
    val converter = Converters.converterFor(schema.elements, -1, collector)

    // each list group only has a single field called element
    override fun getConverter(fieldIndex: Int): Converter {
      require(fieldIndex == 0)
      return converter
    }

    override fun end() {}
    override fun start() {}
  }

  // each array only has a single field called list
  override fun getConverter(fieldIndex: Int): Converter {
    require(fieldIndex == 0)
    return converter
  }

  override fun start() {
    collector.reset()
  }

  override fun end() {
    parent[index] = collector.values()
  }
}
