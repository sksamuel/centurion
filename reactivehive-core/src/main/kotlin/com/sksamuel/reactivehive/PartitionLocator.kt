package com.sksamuel.reactivehive

import org.apache.hadoop.fs.Path

/**
 * Implementations of this interface determine the location of a partition.
 */
interface PartitionLocator {
  fun path(tablePath: Path, partition: Partition): Path
}

/**
 * The default implementation of [PartitionLocator] which uses
 * the same method as the hive metadata.
 */
object DefaultPartitionLocator : PartitionLocator {
  override fun path(tablePath: Path, partition: Partition): Path {
    return partition.parts.fold(tablePath, { path, (key, value) -> Path(path, "${key.value}=$value") })
  }
}
