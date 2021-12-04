package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Record
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.MessageType

class RecordReadSupport : ReadSupport<Record>() {

  override fun init(context: InitContext): ReadContext {
    return ReadContext(context.fileSchema)
  }

  override fun prepareForRead(
    configuration: Configuration,
    keyValueMetaData: MutableMap<String, String>?,
    fileSchema: MessageType,
    readContext: ReadContext,
  ): RecordMaterializer<Record> {
    // convert the incoming parquet schema into a centurion schema type, then
    // use that to create a materializer
    val schema = FromParquetSchema.fromGroupType(fileSchema)
    return RecordRecordMaterializer(schema)
  }
}
