package com.sksamuel.reactivehive.parquet

import com.sksamuel.reactivehive.Struct
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.parquet.converters.StructConverter
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


