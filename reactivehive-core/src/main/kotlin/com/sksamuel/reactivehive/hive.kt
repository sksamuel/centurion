package com.sksamuel.reactivehive

import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table


/**
 * Returns the [PartitionPlan] for the given db.table
 * Returns null if the table is not partitioned
 */
fun partitionPlan(db: DatabaseName,
                  tableName: TableName,
                  client: IMetaStoreClient): PartitionPlan? =
    partitionPlan(client.getTable(db.value, tableName.value))

/**
 * Derives the [PartitionPlan] from the given Hive API table object.
 * Returns null if the table is not partitioned
 */
fun partitionPlan(table: Table): PartitionPlan? {
  val keys = table.partitionKeys?.map { PartitionKey(it.name) } ?: emptyList()
  return if (keys.isEmpty()) null else PartitionPlan(keys)
}

/**
 * Returns the table location from the hive metastore.
 */
fun tableLocation(db: DatabaseName, tableName: TableName, client: IMetaStoreClient): String =
    client.getTable(db.value, tableName.value).sd.location

/**
 * Returns the serde from the hive metastore.
 */
fun serde(db: DatabaseName, tableName: TableName, client: IMetaStoreClient): Serde =
    serde(client.getTable(db.value, tableName.value))

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