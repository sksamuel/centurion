package com.sksamuel.rxhive

import com.sksamuel.rxhive.schemas.FromHiveSchema
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table

class HiveUtils(val client: IMetaStoreClient, val fs: FileSystem) {

  fun listTables(dbName: DatabaseName): List<TableName> {
    return client.getAllTables(dbName.value).map { TableName(it) }
  }

  fun listDatabases(): List<DatabaseName> {
    return client.allDatabases.map { DatabaseName(it) }
  }

  fun table(dbName: DatabaseName, tableName: TableName): Table {
    return client.getTable(dbName.value, tableName.value)
  }

  fun truncateTable(dbName: DatabaseName, tableName: TableName) {
    scanTable(dbName, tableName, client, fs).forEach { fs.delete(it, false) }
  }

  fun schema(dbName: DatabaseName, tableName: TableName): StructType {
    val table = client.getTable(dbName.value, tableName.value)
    return FromHiveSchema.fromHiveTable(table)
  }
}