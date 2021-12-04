package com.sksamuel.centurion.evolution

import com.sksamuel.centurion.DatabaseName
import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.StructType
import com.sksamuel.centurion.TableName
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
 * An implementation of [SchemaEvolver] that is a passthrough operation.
 *
 * Using this implementation means rxhive will make no changes
 * to the metastore. The schema must be managed externally.
 */
object NoopSchemaEvolver : SchemaEvolver {
  override fun evolve(dbName: DatabaseName,
                      tableName: TableName,
                      metastoreSchema: StructType,
                      struct: Struct,
                      client: IMetaStoreClient): StructType = metastoreSchema
}
