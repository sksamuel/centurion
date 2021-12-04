package com.sksamuel.rxhive.parquet

import com.sksamuel.rxhive.Struct
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.schema.MessageType

fun parquetWriter(path: Path,
                  conf: Configuration,
                  schema: MessageType,
                  overwrite: Boolean = false,
                  metadata: Map<String, String> = emptyMap(),
                  settings: ParquetWriterSettings = ParquetWriterSettings()): ParquetWriter<Struct> {

  val writeMode = if (overwrite) ParquetFileWriter.Mode.OVERWRITE else ParquetFileWriter.Mode.CREATE

  return StructParquetWriterBuilder(path, schema, settings.roundingMode, metadata)
      .withCompressionCodec(settings.compressionCodec)
      .withConf(conf)
      //.withDictionaryPageSize()
      .withDictionaryEncoding(settings.dictionaryEncoding)
      .withPageSize(settings.pageSize)
      .withRowGroupSize(settings.rowGroupSize)
      .withValidation(settings.validation)
      .withWriteMode(writeMode)
      .withWriterVersion(settings.writerVersion)
      .build()
}