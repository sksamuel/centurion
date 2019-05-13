package com.sksamuel.rxhive

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.TableType

fun dropTable(dbName: DatabaseName,
              tableName: TableName,
              client: IMetaStoreClient,
              fs: FileSystem) {

  if (client.tableExists(dbName.value, tableName.value)) {

    val table = client.getTable(dbName.value, tableName.value)

    // If the hive table is defined as an external table, then that means we are reponsible for
    // managing all the files, so they must be deleted here
    if (table.tableType == TableType.EXTERNAL_TABLE.asString()) {

      val tableBaseLocation = table.sd.location
      val partitionLocations = client.listPartitions(dbName.value, tableName.value, Short.MAX_VALUE).map { it.sd.location }
      val locations = partitionLocations + tableBaseLocation

      locations.forEach {
        fs.delete(Path(it), true)
      }
    }

    client.dropTable(dbName.value, tableName.value)
  }
}