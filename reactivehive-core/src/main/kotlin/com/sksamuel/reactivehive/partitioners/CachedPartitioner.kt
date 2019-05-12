package com.sksamuel.reactivehive.partitioners

import com.sksamuel.reactivehive.DatabaseName
import com.sksamuel.reactivehive.Partition
import com.sksamuel.reactivehive.PartitionLocator
import com.sksamuel.reactivehive.TableName
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
 * An implementations of [Partitioner] that delegates to an underlying
 * partitioner and caches the result.
 */
class CachedPartitioner(val underlying: Partitioner) : Partitioner {

  private val cache = mutableMapOf<Partition, Path?>()

  override fun path(dbName: DatabaseName,
                    tableName: TableName,
                    partition: Partition,
                    locator: PartitionLocator,
                    client: IMetaStoreClient,
                    fs: FileSystem): Path? {
    return cache.getOrPut(partition) {
      underlying.path(dbName, tableName, partition, locator, client, fs)
    }
  }
}