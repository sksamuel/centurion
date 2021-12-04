package com.sksamuel.centurion.parquet.converters

import com.sksamuel.centurion.StructField
import com.sksamuel.centurion.StructType

class NestedStructConverter(schema: StructType,
                            private val field: StructField,
                            private val parent: MutableMap<String, Any?>) : StructConverter(schema) {
  override fun end() {
    super.end()
    parent[field.name] = currentStruct()
  }
}
