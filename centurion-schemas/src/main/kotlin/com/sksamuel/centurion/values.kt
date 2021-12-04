package com.sksamuel.centurion

data class DRecord(
  val schema: Schema.Record,
  // must be list to ensure ordering is kept to match up with schema
  val values: List<Any>,
) {
  init {
    require(this.schema.fields.size == values.size)
  }
}
