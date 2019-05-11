package com.sksamuel.reactivehive

import com.sksamuel.reactivehive.parquet.ToParquetSchema
import com.sksamuel.reactivehive.parquet.parquetReader
import com.sksamuel.reactivehive.parquet.parquetWriter
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/**
 * [[Format]] encapsulates the ability to read and write files
 * in HDFS in file formats that are compatible with hive.
 *
 * Each implementation will support a different underlying file format.
 * For example, a [[ParquetFormat]] will write files using Apache Parquet,
 * which would be compatible with the
 * org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe serde.
 *
 * Another implementation may use Apache Orc, or Apache Avro, or "your own format".
 * As long as Hive has a Serde, then anything you write out can be read back in hive itself.
 *
 * The idea behind this trait is similar to the
 * org.apache.hadoop.mapreduce.InputFormat interface that hadoop uses.
 */
interface Format {
  fun serde(): Serde
  fun writer(path: Path, schema: StructType, fs: FileSystem): HiveWriter
  fun reader(path: Path, schema: StructType, fs: FileSystem): HiveReader
}

data class Serde(val serializationLib: String,
                 val inputFormat: String,
                 val outputFormat: String,
                 val params: Map<String, String>)

interface HiveWriter {
  fun write(struct: Struct)
  fun close()
}

interface HiveReader {
  fun iterator(): Iterator<Struct>
  fun close()
}

object ParquetFormat : Format {

  override fun serde() = Serde(
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      mapOf("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" to "1")
  )

  override fun writer(path: Path, schema: StructType, fs: FileSystem): HiveWriter = object : HiveWriter {
    // setting overwrite to false, as it should be considered a bug if a hive writer
    // tries to overwrite an existing file
    // logger.debug(s"Creating parquet writer at $path")
    val writer = parquetWriter(
        path,
        fs.conf,
        ToParquetSchema.toMessageType(schema)
    )

    override fun write(struct: Struct): Unit = writer.write(struct)
    override fun close(): Unit = writer.close()
  }

  override fun reader(path: Path, schema: StructType, fs: FileSystem): HiveReader = object : HiveReader {

    //  logger.debug(s"Creating parquet reader for $path")
    val reader = parquetReader(path, fs.conf)

    override fun iterator() = TODO()// = Iterator.continually(reader.read).takeWhile(_ != null)
    override fun close(): Unit = reader.close()
  }
}