package com.sksamuel.rxhive

import com.sksamuel.rxhive.formats.StructReader
import com.sksamuel.rxhive.schemas.FromHiveSchema
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table

class HiveReader(private val dbName: DatabaseName,
                 private val tableName: TableName,
                 private val client: IMetaStoreClient,
                 private val fs: FileSystem) : Logging {

  private val table: Table = client.getTable(dbName.value, tableName.value)
  private val format = serde(table).toFormat()
  private val schema = FromHiveSchema.fromHiveTable(table)

  private val files = scanTable(dbName, tableName, table, client, fs).apply {
    logger.debug("Discovered $size files for table ${dbName.value}.${tableName.value}")
  }

  private var current: StructReader? = null

  fun read(): Sequence<Struct> {
    val seqs = files.map {
      val reader by lazy {
        val reader = format.reader(it, schema, fs.conf)
        current = reader
        reader
      }
      generateSequence {
        val next = reader.read()
        if (next == null) reader.close()
        next
      }
    }
    return seqs.reduce { a, b -> a + b }
  }

  fun close() {
    current?.close()
  }
}

/**
 * Scans a table for all files, including those inside partitions.
 * Looks up partition folders from the metastore.
 */
fun scanTable(dbName: DatabaseName,
              tableName: TableName,
              table: Table,
              client: IMetaStoreClient,
              fs: FileSystem): List<Path> {

  val scanner = DefaultFileScanner
  val partitions = client.listPartitions(dbName.value, tableName.value, Short.MAX_VALUE)

  return if (partitions.isEmpty()) {
    scanner.scan(Path(table.sd.location), fs)
  } else {
    partitions.flatMap {
      scanner.scan(Path(it.sd.location), fs)
    }
  }
}