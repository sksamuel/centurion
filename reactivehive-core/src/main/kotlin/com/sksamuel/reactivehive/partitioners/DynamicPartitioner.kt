package com.sksamuel.reactivehive.partitioners

import com.sksamuel.reactivehive.DatabaseName
import com.sksamuel.reactivehive.Partition
import com.sksamuel.reactivehive.PartitionLocator
import com.sksamuel.reactivehive.TableName
import com.sksamuel.reactivehive.tablePath
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.StorageDescriptor

object DynamicPartitioner : Partitioner {

  override fun path(dbName: DatabaseName,
                    tableName: TableName,
                    partition: Partition,
                    locator: PartitionLocator,
                    client: IMetaStoreClient,
                    fs: FileSystem): Path? {

    val p = client.getPartition(dbName.value, tableName.value, partition.parts.map { it.value })
    val tablePath = tablePath(dbName, tableName, client)
    if (p == null) {

      val table = client.getTable(dbName.value, tableName.value)
      val path = locator.path(tablePath, partition)

      val sd = StorageDescriptor(table.sd)
      sd.location = path.toString()

      val params = mutableMapOf<String, String>()
      val values = partition.parts.map { it.value }
      val ts = (System.currentTimeMillis() / 1000).toInt()

      val p2 = org.apache.hadoop.hive.metastore.api.Partition(values, dbName.value, tableName.value, ts, 0, sd, params)
      client.add_partition(p2)
    }

    return tablePath
  }
}