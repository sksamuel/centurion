package com.sksamuel.centurion

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
 * Scans for paths to read data from.
 * Will scan partitions if they exist, and take into account partition pushdowns.
 */
class TableScanner(private val client: IMetaStoreClient,
                   private val fs: FileSystem) {

  private val scanner = DefaultFileScanner

  fun scan(dbName: DatabaseName, tableName: TableName, pushdown: PartitionPushdown?): List<Path> {

    val table = client.getTable(dbName.value, tableName.value)
    val plan = partitionPlan(table)

    fun scanTable(): List<Path> {
      return scanner.scan(Path(table.sd.location), fs)
    }

    fun scanPartitions(): List<Path> {
      val ps = client.listPartitions(dbName.value, tableName.value, Short.MAX_VALUE)
      val fps = if (pushdown == null || plan == null) ps else ps.filter { pushdown.match(partition(it, plan)) }
      return fps.flatMap { scanner.scan(Path(it.sd.location), fs) }
    }

    return if (plan == null) scanTable() else scanPartitions()
  }
}
