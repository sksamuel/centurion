package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Record
import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.parquet.converters.RecordConverter
import org.apache.parquet.io.api.GroupConverter
import org.apache.parquet.io.api.RecordMaterializer

class RecordRecordMaterializer(schema: Schema.Record) : RecordMaterializer<Record>() {

  private val rootConverter = RecordConverter(schema)

  override fun getCurrentRecord(): Record {
    return rootConverter.currentStruct()
  }

  override fun getRootConverter(): GroupConverter {
    return rootConverter
  }
}
