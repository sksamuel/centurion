package com.sksamuel.reactivehive

/**
 * An implementation of [Partitioner] returns a [Partition] for a [Struct]
 * for a given [PartitionPlan].
 */
interface Partitioner {
  fun partition(struct: Struct, plan: PartitionPlan): Partition
}

object DefaultPartitioner : Partitioner {
  override fun partition(struct: Struct, plan: PartitionPlan): Partition {
    val parts = plan.keys.map {
      val value = struct[it.value]?.toString()
          ?: throw IllegalStateException("Struct $struct does not contain a value for partition key ${it.value}")
      PartitionPart(it, value)
    }
    return Partition(parts)
  }
}