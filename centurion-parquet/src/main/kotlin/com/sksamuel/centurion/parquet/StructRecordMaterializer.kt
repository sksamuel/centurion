package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.parquet.converters.RecordConverter
import org.apache.parquet.io.api.GroupConverter
import org.apache.parquet.io.api.RecordMaterializer

internal class StructRecordMaterializer(schema: Schema.Struct) : RecordMaterializer<Struct>() {

  private val rootConverter = RecordConverter(schema)

  override fun getCurrentRecord(): Struct {
    return rootConverter.currentStruct()
  }

  override fun getRootConverter(): GroupConverter {
    return rootConverter
  }
}
