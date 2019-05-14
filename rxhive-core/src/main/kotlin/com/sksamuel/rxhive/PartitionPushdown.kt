package com.sksamuel.rxhive

/**
 * A clause that can be pushed down to the partitions level, skipping
 * entire partitions.
 */
interface PartitionPushdown {

  fun match(partition: Partition): Boolean

  class EqualsPartitionPushdown(val key: String, val value: String) : PartitionPushdown {
    override fun match(partition: Partition): Boolean =
        partition.parts.contains(PartitionPart(PartitionKey(key), value))
  }

  class NotEqualsPartitionPushdown(val key: String, val value: String) : PartitionPushdown {
    override fun match(partition: Partition): Boolean = !EqualsPartitionPushdown(key, value).match(partition)
  }

  companion object {
    fun eq(key: String, value: String): PartitionPushdown = EqualsPartitionPushdown(key, value)
    fun ne(key: String, value: String): PartitionPushdown = NotEqualsPartitionPushdown(key, value)

    fun and(left: PartitionPushdown,
            right: PartitionPushdown): PartitionPushdown = object : PartitionPushdown {
      override fun match(partition: Partition): Boolean = left.match(partition) && right.match(partition)
    }

    fun or(left: PartitionPushdown, right: PartitionPushdown): PartitionPushdown = object : PartitionPushdown {
      override fun match(partition: Partition): Boolean = left.match(partition) || right.match(partition)
    }
  }
}
