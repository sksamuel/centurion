package com.sksamuel.rxhive.formats

import com.sksamuel.rxhive.Struct
import com.sksamuel.rxhive.StructType
import com.sksamuel.rxhive.parquet.ToParquetSchema
import com.sksamuel.rxhive.parquet.parquetReader
import com.sksamuel.rxhive.parquet.parquetWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object ParquetFormat : Format {

  override fun serde() = Serde(
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      mapOf("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" to "1")
  )

  override fun writer(path: Path, schema: StructType, conf: Configuration): StructWriter = object : StructWriter {
    // setting overwrite to false, as it should be considered a bug if a hive writer
    // tries to overwrite an existing file
    // logger.debug(s"Creating parquet writer at $path")
    val writer = parquetWriter(
        path,
        conf,
        ToParquetSchema.toMessageType(schema)
    )

    override fun write(struct: Struct): Unit = writer.write(struct)
    override fun close(): Unit = writer.close()
  }

  override fun reader(path: Path, schema: StructType, conf: Configuration): StructReader = object : StructReader {
    val reader = parquetReader(path, conf)
    override fun read() = reader.read()
    override fun close(): Unit = reader.close()
  }
}