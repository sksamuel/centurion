package com.sksamuel.reactivehive

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.StorageDescriptor

/**
 * A [Partitioner] is used to create the partitions needed by hive writers.
 *
 * More specifically, it will create directories on the file system,
 * as well as updating the hive metastore with new partition information.
 *
 * Typically, partition strategies include dynamic partitioning, where the
 * system will create new partitions as required, and static partitioning, where
 * partitions are required to be created in advance otherwise an error is throw.
 * (see https://cwiki.apache.org/confluence/display/Hive/DynamicPartitions).
 *
 * The pre-existing partitions cover both these scenarios in the form of
 * [DynamicPartitioner] and [StaticPartitioner].
 *
 */
interface Partitioner {

  fun path(dbName: DatabaseName,
           tableName: TableName,
           partition: Partition,
           locator: PartitionLocator,
           client: IMetaStoreClient,
           fs: FileSystem): Path?
}

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