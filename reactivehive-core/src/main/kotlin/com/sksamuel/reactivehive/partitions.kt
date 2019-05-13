package com.sksamuel.reactivehive

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