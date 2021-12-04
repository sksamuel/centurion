package com.sksamuel.centurion.parquet

import org.apache.hadoop.fs.permission.FsPermission
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
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

