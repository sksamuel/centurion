package com.sksamuel.reactivehive

import org.apache.hadoop.hive.metastore.IMetaStoreClient

class HiveUtils(val client: IMetaStoreClient) {

  fun listTables(dbName: DatabaseName): List<TableName> {
    return client.getAllTables(dbName.value).map { TableName(it) }
  }

  fun listDatabases(): List<DatabaseName> {
    return client.allDatabases.map { DatabaseName(it) }
  }

}