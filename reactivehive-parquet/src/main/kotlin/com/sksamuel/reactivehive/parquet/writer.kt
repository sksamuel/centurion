package com.sksamuel.reactivehive.parquet

import com.sksamuel.reactivehive.Struct
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType
import java.math.RoundingMode

data class ParquetWriterSettings(val compressionCodec: CompressionCodecName = CompressionCodecName.SNAPPY,
                                 val dictionaryEncoding: Boolean = ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                                 val permission: FsPermission? = null,
                                 val inheritPermissions: Boolean = false,
                                 val validation: Boolean = ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                                 val rowGroupSize: Int = ParquetWriter.DEFAULT_BLOCK_SIZE,
                                 val roundingMode: RoundingMode = RoundingMode.UNNECESSARY,
                                 val writerVersion: ParquetProperties.WriterVersion = ParquetProperties.WriterVersion.PARQUET_1_0,
                                 val pageSize: Int = ParquetProperties.DEFAULT_PAGE_SIZE)


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
      .withDictionaryEncoding(settings.dictionaryEncoding)
      .withPageSize(settings.pageSize)
      .withRowGroupSize(settings.rowGroupSize)
      .withValidation(settings.validation)
      .withWriteMode(writeMode)
      .withWriterVersion(settings.writerVersion)
      .build()
}

class StructParquetWriterBuilder(path: Path,
                                 private val schema: MessageType,
                                 private val roundingMode: RoundingMode,
                                 private val meta: Map<String, String>) : ParquetWriter.Builder<Struct, StructParquetWriterBuilder>(
    path) {
  override fun getWriteSupport(conf: Configuration) = StructWriteSupport(schema, roundingMode, meta)
  override fun self(): StructParquetWriterBuilder = this
}

class StructWriteSupport(private val schema: MessageType,
                         private val roundingMode: RoundingMode,
                         private val metadata: Map<String, String>) : WriteSupport<Struct>() {

  override fun init(configuration: Configuration?) = WriteContext(schema, metadata)

  override fun finalizeWrite(): FinalizedWriteContext = FinalizedWriteContext(metadata)

  private var consumer: RecordConsumer? = null

  override fun prepareForWrite(consumer: RecordConsumer) {
    this.consumer = consumer
  }

  override fun write(struct: Struct) {
    consumer?.let {
      StructSetter(struct.schema, RoundingMode.UNNECESSARY, true).set(it, struct)
    }
  }
}