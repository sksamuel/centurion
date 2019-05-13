package com.sksamuel.reactivehive.resolver

import arrow.core.Tuple2
import com.sksamuel.reactivehive.DatabaseName
import com.sksamuel.reactivehive.Struct
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.TableName
import com.sksamuel.reactivehive.align
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
 * An implementation of [SchemaResolver] that will return a struct that is aligned
 * with the metastore schema by dropping fields present in the struct but not
 * present in the metastore, and padding fields in the metastore but not present
 * in the struct.
 *
 * In other words, if a metastore has schema { a, c } and a struct has
 * schema { b, c, d } then fields { b, d } will be removed from the
 * incoming struct so that it matches the metastore. Field { a } will
 * then be added with a value of null.
 *
 * Any fields in the metastore that are not present in the struct must
 * be marked as nullable or an error will be thrown.
 *
 * This implementation will never make any changes to the metastore.
 */
object MetastoreAlignedSchemaResolver : SchemaResolver {

  override fun align(dbName: DatabaseName,
                     tableName: TableName,
                     metastoreSchema: StructType,
                     struct: Struct,
                     client: IMetaStoreClient): Tuple2<Boolean, Struct> {
    val aligned = align(struct, metastoreSchema)
    return Tuple2(false, aligned)
  }
}