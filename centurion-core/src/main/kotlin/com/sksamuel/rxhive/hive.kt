package com.sksamuel.rxhive

import com.sksamuel.rxhive.formats.Format
import com.sksamuel.rxhive.formats.ParquetFormat
import com.sksamuel.rxhive.formats.Serde
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table

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

fun Serde.toFormat(): Format = when {
  this.inputFormat == ParquetFormat.serde().inputFormat -> ParquetFormat
  else -> throw UnsupportedOperationException("Unknown serde $this")
}