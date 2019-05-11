package com.sksamuel.reactivehive

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table
import java.util.*

/**
 * Returns the [PartitionPlan] for the given db.table
 * Returns null if the table is not partitioned.
 */
fun partitionPlan(dbName: DatabaseName,
                  tableName: TableName,
                  client: IMetaStoreClient): PartitionPlan? =
    partitionPlan(client.getTable(dbName.value, tableName.value))

/**
 * Derives the [PartitionPlan] from the given Hive API table object.
 * Returns null if the table is not partitioned.
 */
fun partitionPlan(table: Table): PartitionPlan? {
  val keys = table.partitionKeys?.map { PartitionKey(it.name) } ?: emptyList()
  return if (keys.isEmpty()) null else PartitionPlan(keys)
}

/**
 * Returns the table location from the hive metastore.
 */
fun tablePath(dbName: DatabaseName, tableName: TableName, client: IMetaStoreClient): Path =
    tablePath(client.getTable(dbName.value, tableName.value))

fun tablePath(table: Table): Path = Path(table.sd.location)

/**
 * Returns the serde from the hive metastore.
 */
fun serde(dbName: DatabaseName, tableName: TableName, client: IMetaStoreClient): Serde =
    serde(client.getTable(dbName.value, tableName.value))

/**
 * Derives the [Serde] for a table from the Hive API table object.
 */
fun serde(table: Table): Serde =
    Serde(
        table.sd.serdeInfo.serializationLib,
        table.sd.inputFormat,
        table.sd.outputFormat,
        table.sd.serdeInfo.parameters ?: emptyMap()
    )

/**
 * Returns the path to the file that this struct should be written to.
 * If the table is partitioned then this will be located in the partition path,
 * otherwise in base table location.
 */
fun outputFile(struct: Struct, plan: PartitionPlan?, table: Table, locator: PartitionLocator): Path {
  val tableBasePath = Path(table.sd.location)
  return when (plan) {
    null -> tableBasePath
    else -> {
      val partition = partition(struct, plan)
      val dir = locator.path(tableBasePath, partition)
      Path(dir, UUID.randomUUID().toString())
    }
  }
}