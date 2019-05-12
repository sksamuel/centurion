package com.sksamuel.reactivehive.partitioners

import com.sksamuel.reactivehive.DatabaseName
import com.sksamuel.reactivehive.Partition
import com.sksamuel.reactivehive.TableName
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
 * A [Partitioner] is used to create the partitions needed when writing to hive.
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
 * Users are free to implement their own strategies, for example if they wish
 * the location of partitions to be different from the default hive mechanism.
 *
 */
interface Partitioner {

  /**
   * Returns the [Path] where data for the given partition should be written.
   * After returning, the path should exist, and the hive metastore should
   * be aware of it's existence.
   */
  fun path(dbName: DatabaseName,
           tableName: TableName,
           partition: Partition,
           client: IMetaStoreClient,
           fs: FileSystem): Path
}

