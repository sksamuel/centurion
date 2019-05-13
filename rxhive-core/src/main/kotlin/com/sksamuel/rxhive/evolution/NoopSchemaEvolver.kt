package com.sksamuel.rxhive.evolution

import com.sksamuel.rxhive.DatabaseName
import com.sksamuel.rxhive.Struct
import com.sksamuel.rxhive.StructType
import com.sksamuel.rxhive.TableName
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