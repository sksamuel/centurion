package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.parquet.schemas.ToParquetSchema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.util.HadoopInputFile
import java.io.File

object Parquet {

  fun count(paths: List<Path>, conf: Configuration): Long {
    return paths.sumOf { path ->
      val input = HadoopInputFile.fromPath(path, conf)
      val reader = ParquetFileReader.open(input)
      reader.footer.blocks.sumOf { it.rowCount }
    }
  }

  /**
   * Create a [ParquetReader] for a Hadoop [Path].
   */
  fun reader(path: Path, conf: Configuration): ParquetReader<Struct> {
    return ParquetReader.builder(StructReadSupport(), path)
      .withConf(conf)
      .build()
  }

  /**
   * Create a [ParquetReader] for a local [File].
   */
  fun reader(file: File, conf: Configuration): ParquetReader<Struct> {
    return ParquetReader.builder(StructReadSupport(), Path("file://" + file.path))
      .withConf(conf)
      .build()
  }

  fun writer(
    path: java.nio.file.Path,
    conf: Configuration,
    schema: Schema.Struct,
    mode: ParquetFileWriter.Mode = ParquetFileWriter.Mode.CREATE,
    metadata: Map<String, String> = emptyMap(),
    settings: ParquetWriterSettings = ParquetWriterSettings(),
  ): ParquetWriter<Struct> {
    return writer(
      Path("file://$path"),
      conf,
      schema,
      mode = mode,
      metadata = metadata,
      settings = settings,
    )
  }

  fun writer(
    path: Path,
    conf: Configuration,
    schema: Schema.Struct,
    mode: ParquetFileWriter.Mode = ParquetFileWriter.Mode.CREATE,
    metadata: Map<String, String> = emptyMap(),
    settings: ParquetWriterSettings = ParquetWriterSettings(),
  ): ParquetWriter<Struct> {

    return StructParquetWriterBuilder(path, ToParquetSchema.toMessageType(schema), settings.roundingMode, metadata)
      .withCompressionCodec(settings.compressionCodec)
      .withConf(conf)
      //.withDictionaryPageSize()
      .withDictionaryEncoding(settings.dictionaryEncoding)
      .withPageSize(settings.pageSize)
      .withRowGroupSize(settings.rowGroupSize)
      .withValidation(settings.validation)
      .withWriteMode(mode)
      .withWriterVersion(settings.writerVersion)
      .build()
  }
}

fun <T : Any> ParquetReader<T>.sequence(): Sequence<T> = generateSequence { read() }
fun <T : Any> ParquetReader<T>.readAll(): List<T> = sequence().toList()

fun <T> ParquetWriter<T>.write(ts: List<T>) = ts.forEach { write(it) }
