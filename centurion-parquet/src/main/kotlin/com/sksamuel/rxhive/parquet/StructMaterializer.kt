package com.sksamuel.rxhive.parquet

import com.sksamuel.rxhive.Struct
import com.sksamuel.rxhive.StructType
import com.sksamuel.rxhive.parquet.converters.StructConverter
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


