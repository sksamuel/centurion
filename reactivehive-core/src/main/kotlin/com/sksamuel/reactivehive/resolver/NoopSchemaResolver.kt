package com.sksamuel.reactivehive.resolver

import arrow.core.Tuple2
import com.sksamuel.reactivehive.DatabaseName
import com.sksamuel.reactivehive.Struct
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.TableName
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
 * An implementation of [SchemaResolver] that is a passthrough operation.
 *
 * Using this implementation means reactive-hive will make no changes
 * to the metastore or the struct. The schema must be managed externally.
 *
 * This implementation can result in structs that are not compatible
 * with the metastore. Use of this implementation is NOT recommended in
 * production.
 */
object NoopSchemaResolver : SchemaResolver {
  override fun align(dbName: DatabaseName,
                     tableName: TableName,
                     metastoreSchema: StructType,
                     struct: Struct,
                     client: IMetaStoreClient) = Tuple2(false, struct)
}