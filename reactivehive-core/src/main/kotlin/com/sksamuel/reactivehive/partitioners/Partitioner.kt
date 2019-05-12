package com.sksamuel.reactivehive.partitioners

import com.sksamuel.reactivehive.DatabaseName
import com.sksamuel.reactivehive.Partition
import com.sksamuel.reactivehive.PartitionLocator
import com.sksamuel.reactivehive.TableName
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient

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

