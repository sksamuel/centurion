package com.sksamuel.rxhive.partitioners

import com.sksamuel.rxhive.DatabaseName
import com.sksamuel.rxhive.Partition
import com.sksamuel.rxhive.TableName
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
 * An implementations of [Partitioner] that delegates to an underlying
 * partitioner and caches the result.
 */
class CachedPartitioner(val underlying: Partitioner) : Partitioner {

  private val cache = mutableMapOf<Partition, Path>()

  override fun path(dbName: DatabaseName,
                    tableName: TableName,
                    partition: Partition,
                    client: IMetaStoreClient,
                    fs: FileSystem): Path {
    return cache.getOrPut(partition) {
      underlying.path(dbName, tableName, partition, client, fs)
    }
  }
}