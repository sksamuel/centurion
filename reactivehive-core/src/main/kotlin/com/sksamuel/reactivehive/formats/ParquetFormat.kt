package com.sksamuel.reactivehive.formats

import com.sksamuel.reactivehive.Struct
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.parquet.ToParquetSchema
import com.sksamuel.reactivehive.parquet.parquetReader
import com.sksamuel.reactivehive.parquet.parquetWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object ParquetFormat : Format {

  override fun serde() = Serde(
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      mapOf("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" to "1")
  )

  override fun writer(path: Path, schema: StructType, conf: Configuration): HiveWriter = object :
      HiveWriter {
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

  override fun reader(path: Path, schema: StructType, conf: Configuration): StructReader = object :
      StructReader {

    //  logger.debug(s"Creating parquet reader for $path")
    val reader = parquetReader(path, conf)

    override fun iterator() = TODO()// = Iterator.continually(reader.read).takeWhile(_ != null)
    override fun close(): Unit = reader.close()
  }
}