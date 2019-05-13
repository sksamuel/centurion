package com.sksamuel.reactivehive

/**
 * Usually when writing to a partitioned table, the partition values do not need to be included
 * in the written data. This is because their values are static for all structs in the same
 * partition, and can therefore be derived from the partition they are in.
 *
 * For example, if a table is partitioned by country, then all files located in the "UK"
 * partition clearly have country=UK, and so there's little point writing that out for
 * every single row.
 *
 * Therefore, implementations of this interface are used to derive the schema that will
 * be written to disk. The default implementation drops all partitioned fields, but custom
 * implementations are free to implement any behavior they wish. Remember though, that
 * any data written will need to be compatible with other systems such as Spark and Impala.
 */
interface DiskSchemas {
  fun writerSchema(struct: Struct, metastoreSchema: StructType, partition: PartitionPlan): StructType
  fun expand(struct: Struct, partition: Partition): Struct
}

object DefaultDiskSchemas : DiskSchemas {

  override fun writerSchema(struct: Struct,
                            metastoreSchema: StructType,
                            partition: PartitionPlan): StructType {
    val fields = metastoreSchema.fields.filterNot { partition.keys.contains(PartitionKey(it.name)) }
    return StructType(fields)
  }

  override fun expand(struct: Struct, partition: Partition): Struct {
    return struct
  }
}