package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Struct
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.schema.MessageType
import java.math.RoundingMode

internal class StructParquetWriterBuilder(
  path: Path,
  private val schema: MessageType,
  private val roundingMode: RoundingMode,
  private val meta: Map<String, String>,
) : ParquetWriter.Builder<Struct, StructParquetWriterBuilder>(path) {

  override fun getWriteSupport(conf: Configuration): WriteSupport<Struct> =
    StructWriteSupport(schema, roundingMode, meta)

  override fun self(): StructParquetWriterBuilder = this
}
