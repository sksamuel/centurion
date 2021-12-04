package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.StructType
import com.sksamuel.centurion.parquet.converters.StructConverter
import org.apache.parquet.io.api.GroupConverter
import org.apache.parquet.io.api.RecordMaterializer

/**
 * Materializes [Struct]s from a parquet stream.
 */
class StructMaterializer(schema: StructType) : RecordMaterializer<Struct>() {

  private val rootConverter = StructConverter(schema)

  override fun getRootConverter(): GroupConverter = rootConverter

  override fun getCurrentRecord(): Struct = rootConverter.currentStruct()
}


