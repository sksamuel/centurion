package com.sksamuel.reactivehive.processors

import com.sksamuel.reactivehive.PartitionKey
import com.sksamuel.reactivehive.PartitionPlan
import com.sksamuel.reactivehive.Struct
import com.sksamuel.reactivehive.StructType

/**
 * This [StructTransformer] returns a [Struct] with the partition values removed.
 *
 * Usually, partition values do not need to be included in the data
 * written to disk. This is because the partition values can usually
 * be inferred from the partition directory name.
 *
 * For example, if a file contained structs for a partition "city=chicago",
 * then it would normally be in a file located in a directory with that name.
 * So the partition value could be extracted by examining the directory name.
 *
 * This reduces the amount of IO needed, and keeps the file sizes smaller.
 *
 * If you are using a custom [PartitionLocator] that does not include the partition
 * values in the directory name, then you may wish to keep the partition values in
 * the file. If so, this processor should be removed.
 *
 */
class StripPartitionsStructTransformer(val plan: PartitionPlan) : StructTransformer {

  override fun process(struct: Struct): Struct {
    val fields = struct.schema.fields.mapNotNull {
      if (plan.keys.contains(PartitionKey(it.name))) null else it
    }
    val values = fields.map { struct[it.name] }
    return Struct(StructType(fields), values)
  }
}