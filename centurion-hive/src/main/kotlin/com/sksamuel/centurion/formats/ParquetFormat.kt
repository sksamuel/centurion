package com.sksamuel.centurion.formats

import com.sksamuel.centurion.Logging
import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.StructType
import com.sksamuel.centurion.parquet.ToParquetSchema
import com.sksamuel.centurion.parquet.parquetReader
import com.sksamuel.centurion.parquet.parquetWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object ParquetFormat : Format, Logging {

  override fun serde() = Serde(
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      mapOf("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" to "1")
  )

  override fun writer(path: Path,
                      schema: StructType,
                      conf: Configuration): StructWriter = object : StructWriter, Logging {

    // setting overwrite to false, as it should be considered a bug if a hive writer
    // tries to overwrite an existing file
    val writer by lazy {
      logger.debug("Creating parquet writer at $path")
      parquetWriter(
          path,
          conf,
          schema = ToParquetSchema.toMessageType(schema),
          overwrite = false
      )
    }

    override fun write(struct: Struct): Unit = writer.write(struct)
    override fun close(): Unit = writer.close()
  }

  override fun reader(path: Path, schema: StructType, conf: Configuration): StructReader = object : StructReader {
    val reader = parquetReader(path, conf)
    override fun read() = reader.read()
    override fun close(): Unit = reader.close()
  }
}
