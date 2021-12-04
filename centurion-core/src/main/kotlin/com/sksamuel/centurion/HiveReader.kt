package com.sksamuel.centurion

import com.sksamuel.centurion.formats.StructReader
import com.sksamuel.centurion.schemas.FromHiveSchema
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table

class HiveReader(private val dbName: DatabaseName,
                 private val tableName: TableName,
                 private val pushdown: PartitionPushdown?,
                 private val client: IMetaStoreClient,
                 private val fs: FileSystem) : Logging {

  private val table: Table = client.getTable(dbName.value, tableName.value)
  private val format = serde(table).toFormat()
  private val schema = FromHiveSchema.fromHiveTable(table)
  private val scanner = TableScanner(client, fs)
  private val files = scanner.scan(dbName, tableName, pushdown).apply {
    logger.debug("Discovered $size files for table ${dbName.value}.${tableName.value}")
  }

  private var index = 0
  private var current: StructReader? = null

  private fun advanceReader() {
    current = if (index == files.size) null else {
      format.reader(files[index++], schema, fs.conf)
    }
  }

  fun read(): Struct? {

    if (current == null) {
      advanceReader()
      if (current == null)
        return null
    }

    var next = current?.read()
    while (next == null) {
      advanceReader()
      if (current == null)
        return null
      next = current?.read()
    }

    return next
  }

  fun close() {
    current?.close()
  }
}
