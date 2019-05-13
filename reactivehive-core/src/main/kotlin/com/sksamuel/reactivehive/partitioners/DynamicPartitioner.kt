package com.sksamuel.reactivehive.partitioners

import com.sksamuel.reactivehive.DatabaseName
import com.sksamuel.reactivehive.Partition
import com.sksamuel.reactivehive.TableName
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.hadoop.hive.metastore.api.StorageDescriptor

/**
 * A hive [Partitioner] that creates partitions on the fly using the default hive mechanism.
 * That is, a partition consisting of { a -> b, c -> d } will be located in a directory
 * with the path ../a=b/c=d/..
 *
 * If you wish to create partitions on the fly, but in a unique location, then create an
 * implementation similar to this, but change the way the paths are constructed.
 */
object DynamicPartitioner : Partitioner {

  override fun path(dbName: DatabaseName,
                    tableName: TableName,
                    partition: Partition,
                    client: IMetaStoreClient,
                    fs: FileSystem): Path {

    // the partition is fetched using values only, as the order matters in the hive metastore
    try {
      val p = client.getPartition(dbName.value, tableName.value, partition.parts.map { it.value })
      return Path(p.sd.location)
    } catch (e: NoSuchObjectException) {

      val table = client.getTable(dbName.value, tableName.value)
      val tablePath = Path(table.sd.location)

      // the full path to the partition, which will be located inside the table root
      val path = partition.parts.fold(tablePath, { path, (key, value) -> Path(path, "${key.value}=$value") })

      val sd = StorageDescriptor(table.sd)
      sd.location = path.toString()

      val params = mutableMapOf<String, String>()
      val values = partition.parts.map { it.value }
      val ts = (System.currentTimeMillis() / 1000).toInt()

      val p2 = org.apache.hadoop.hive.metastore.api.Partition(values, dbName.value, tableName.value, ts, 0, sd, params)
      client.add_partition(p2)

      return path
    }
  }
}