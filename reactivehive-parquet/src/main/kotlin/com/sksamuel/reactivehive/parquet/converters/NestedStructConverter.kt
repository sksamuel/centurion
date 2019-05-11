package com.sksamuel.reactivehive.parquet.converters

import com.sksamuel.reactivehive.StructField
import com.sksamuel.reactivehive.StructType

class NestedStructConverter(schema: StructType,
                            private val field: StructField,
                            private val parent: MutableMap<String, Any?>) : StructConverter(schema) {
  override fun end() {
    super.end()
    parent[field.name] = currentStruct()
  }
}