package com.sksamuel.centurion.parquet

import org.apache.parquet.hadoop.ParquetReader

fun <T : Any> ParquetReader<T>.readAll(): Sequence<T> = generateSequence { read() }
