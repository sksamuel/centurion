package com.sksamuel.centurion

import com.sksamuel.centurion.schemas.FromHiveSchema
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.TableType
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
    val scanner = TableScanner(client, fs)
    scanner.scan(dbName, tableName, null).forEach { fs.delete(it, false) }
  }

  fun schema(dbName: DatabaseName, tableName: TableName): StructType {
    val table = table(dbName, tableName)
    return FromHiveSchema.fromHiveTable(table)
  }

  fun count(dbName: DatabaseName, tableName: TableName): Long {
    val scanner = TableScanner(client, fs)
    val paths = scanner.scan(dbName, tableName, null)
    return com.sksamuel.centurion.parquet.count(paths, fs.conf)
  }

  fun partitionFields(dbName: DatabaseName, tableName: TableName): List<StructField> {
    val table = table(dbName, tableName)
    return table.partitionKeys.map { StructField(it.name, FromHiveSchema.fromHiveType(it.type), false) }
  }

  fun buckets(dbName: DatabaseName, tableName: TableName): List<String> {
    val table = table(dbName, tableName)
    return table.sd.bucketCols
  }

  fun files(dbName: DatabaseName, tableName: TableName): List<Path> {
    val scanner = TableScanner(client, fs)
    return scanner.scan(dbName, tableName, null)
  }

  fun dropTable(dbName: DatabaseName,
                tableName: TableName,
                deleteFiles: Boolean) {

    if (client.tableExists(dbName.value, tableName.value)) {

      val table = client.getTable(dbName.value, tableName.value)

      // If the hive table is defined as an external table, then that means we are reponsible for
      // managing all the files, so they must be deleted here
      if (deleteFiles && table.tableType == TableType.EXTERNAL_TABLE.asString()) {

        val tableBaseLocation = table.sd.location
        val partitionLocations = client.listPartitions(dbName.value, tableName.value, Short.MAX_VALUE).map { it.sd.location }
        val locations = partitionLocations + tableBaseLocation

        locations.forEach {
          fs.delete(Path(it), true)
        }
      }

      client.dropTable(dbName.value, tableName.value, deleteFiles, true)
    }
  }
}
