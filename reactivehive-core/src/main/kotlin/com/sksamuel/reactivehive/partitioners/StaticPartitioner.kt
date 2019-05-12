package com.sksamuel.reactivehive.partitioners

import com.sksamuel.reactivehive.DatabaseName
import com.sksamuel.reactivehive.Partition
import com.sksamuel.reactivehive.PartitionLocator
import com.sksamuel.reactivehive.TableName
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient

object StaticPartitioner : Partitioner {

  override fun path(dbName: DatabaseName,
                    tableName: TableName,
                    partition: Partition,
                    locator: PartitionLocator,
                    client: IMetaStoreClient,
                    fs: FileSystem): Path? {
    return try {
      client.getPartition(dbName.value, tableName.value, partition.parts.map { it.value })
      null
    } catch (t: Throwable) {
      throw RuntimeException("Partition $partition does not exist and static partitioner requires upfront creation", t)
    }
  }
}