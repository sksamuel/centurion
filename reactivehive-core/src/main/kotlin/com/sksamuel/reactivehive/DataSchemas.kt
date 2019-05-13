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
interface DataSchemas {
  fun writerSchema(struct: Struct, partition: PartitionPlan): Struct
}

object DefaultDataSchemas : DataSchemas {

  override fun writerSchema(struct: Struct,
                            partition: PartitionPlan): Struct {
    return struct
  }

}