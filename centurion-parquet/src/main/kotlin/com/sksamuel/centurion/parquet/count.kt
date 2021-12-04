package com.sksamuel.centurion.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

fun count(paths: List<Path>, conf: Configuration): Long {
  return paths.map { path ->
    val input = HadoopInputFile.fromPath(path, conf)
    val reader = ParquetFileReader.open(input)
    reader.footer.blocks.map { it.rowCount }.sum()
  }.sum()
}
