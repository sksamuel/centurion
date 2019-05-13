package com.sksamuel.rxhive.processors

import com.sksamuel.rxhive.Partition
import com.sksamuel.rxhive.Struct

/**
 * The counterpoint to the [StripPartitionsStructTransformer].
 *
 * After a struct has been read from the filesystem, it is usually the case
 * that the partition values are missing. That is, they will not have been present
 * in the data file, but instead are inferred from the file location.
 *
 * This processor will transform structs to include the partition values.
 */
class PopulatePartitionsStructTransformer(partition: Partition) : StructTransformer {
  override fun process(struct: Struct): Struct = TODO()
}