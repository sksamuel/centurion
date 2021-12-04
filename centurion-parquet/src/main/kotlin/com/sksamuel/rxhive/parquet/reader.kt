package com.sksamuel.rxhive.parquet

import com.sksamuel.rxhive.Struct
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.MessageType

object StructReadSupport : ReadSupport<Struct>() {

  override fun prepareForRead(configuration: Configuration,
                              keyValueMetaData: MutableMap<String, String>,
                              fileSchema: MessageType,
                              readContext: ReadContext): RecordMaterializer<Struct> {
    val schema = FromParquetSchema.fromGroupType(fileSchema)
    return StructMaterializer(schema)
  }

  override fun init(context: InitContext): ReadContext = ReadContext(context.fileSchema)
}

fun <T : Any> ParquetReader<T>.readAll(): Sequence<T> = generateSequence { read() }