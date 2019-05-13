package com.sksamuel.reactivehive.evolution

import com.sksamuel.reactivehive.DatabaseName
import com.sksamuel.reactivehive.Struct
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.TableName
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
 * An implementation of [SchemaEvolver] that is a passthrough operation.
 *
 * Using this implementation means reactive-hive will make no changes
 * to the metastore. The schema must be managed externally.
 */
object NoopSchemaEvolver : SchemaEvolver {
  override fun evolve(dbName: DatabaseName,
                      tableName: TableName,
                      metastoreSchema: StructType,
                      struct: Struct,
                      client: IMetaStoreClient): StructType = metastoreSchema
}