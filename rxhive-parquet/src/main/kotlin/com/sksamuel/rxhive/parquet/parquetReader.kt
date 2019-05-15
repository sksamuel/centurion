package com.sksamuel.rxhive.parquet

import com.sksamuel.rxhive.Struct
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetReader

fun parquetReader(path: Path, conf: Configuration): ParquetReader<Struct> {
  return ParquetReader.builder(StructReadSupport, path)
      .withConf(conf)
      .build()
}