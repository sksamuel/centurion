package com.sksamuel.rxhive.parquet.converters

import com.sksamuel.rxhive.StructField
import com.sksamuel.rxhive.StructType

class NestedStructConverter(schema: StructType,
                            private val field: StructField,
                            private val parent: MutableMap<String, Any?>) : StructConverter(schema) {
  override fun end() {
    super.end()
    parent[field.name] = currentStruct()
  }
}