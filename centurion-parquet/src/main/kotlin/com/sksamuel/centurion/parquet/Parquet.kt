package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Struct
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.MessageType

object Parquet {

  fun count(paths: List<Path>, conf: Configuration): Long {
    return paths.sumOf { path ->
      val input = HadoopInputFile.fromPath(path, conf)
      val reader = ParquetFileReader.open(input)
      reader.footer.blocks.sumOf { it.rowCount }
    }
  }

  fun reader(path: Path, conf: Configuration): ParquetReader<Struct> {
    return ParquetReader.builder(StructReadSupport(), path)
      .withConf(conf)
      .build()
  }

  fun writer(
    path: Path,
    conf: Configuration,
    schema: MessageType,
    overwrite: Boolean = false,
    metadata: Map<String, String> = emptyMap(),
    settings: ParquetWriterSettings = ParquetWriterSettings()
  ): ParquetWriter<Struct> {

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
}

fun <T : Any> ParquetReader<T>.sequence(): Sequence<T> = generateSequence { read() }
