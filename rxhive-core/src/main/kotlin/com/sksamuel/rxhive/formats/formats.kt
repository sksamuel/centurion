package com.sksamuel.rxhive.formats

import com.sksamuel.rxhive.Struct
import com.sksamuel.rxhive.StructType
import org.apache.hadoop.conf.Configuration
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
  fun writer(path: Path, schema: StructType, conf: Configuration): StructWriter
  fun reader(path: Path, schema: StructType, conf: Configuration): StructReader
}

data class Serde(val serializationLib: String,
                 val inputFormat: String,
                 val outputFormat: String,
                 val params: Map<String, String>)

interface StructWriter {
  fun write(struct: Struct)
  fun close()
}

interface StructReader {
  fun read(): Struct?
  fun close()
}
