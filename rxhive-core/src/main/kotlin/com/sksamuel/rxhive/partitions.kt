package com.sksamuel.rxhive

/**
 * Creates a [Partition] for the given struct and plan.
 */
fun partition(struct: Struct, plan: PartitionPlan): Partition {
  val parts = plan.keys.map {
    val value = struct[it.value]?.toString()
        ?: throw IllegalStateException("Struct $struct does not contain a value for partition key ${it.value}")
    PartitionPart(it, value)
  }
  return Partition(parts)
}

/**
 * Creates an rxhive partition from the hive api partition
 */
fun partition(partition: org.apache.hadoop.hive.metastore.api.Partition, plan: PartitionPlan): Partition {
  return Partition(plan.keys.zip(partition.values).map { PartitionPart(it.first, it.second) })
}