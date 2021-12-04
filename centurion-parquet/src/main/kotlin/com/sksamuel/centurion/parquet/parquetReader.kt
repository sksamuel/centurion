package com.sksamuel.centurion.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetReader
import com.sksamuel.centurion.Record

fun parquetReader(path: Path, conf: Configuration): ParquetReader<Record> {
  return ParquetReader.builder(RecordReadSupport(), path)
    .withConf(conf)
    .build()
}
